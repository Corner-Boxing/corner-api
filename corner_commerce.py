"""Corner web commerce v1. All keys are server-side. Stripe REST pinned to 2024-06-20.
Install with install_commerce(app, supabase, verify_user, legacy_plan, developer_check).
"""
import os
import time
import json
import hmac
import hashlib
import secrets
import html
import re
from datetime import datetime, timezone, timedelta, date
from contextlib import contextmanager
from urllib.parse import urlencode
import requests
from flask import request, jsonify, Response, redirect

VERSION = '2026-11-13'
STRIPE_VERSION = '2024-06-20'
TERMINAL = {'canceled', 'incomplete_expired'}

class CommerceError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status

def now():
    return datetime.now(timezone.utc)

def iso(value=None):
    return (value or now()).isoformat()

def dt(value):
    if not value:
        return None
    if isinstance(value, (float, int)):
        return datetime.fromtimestamp(value, timezone.utc)
    return datetime.fromisoformat(value.replace('Z', '+00:00'))

def objid(value):
    return value.get('id') if isinstance(value, dict) else value

def age_band(birth):
    try:
        born = date.fromisoformat(str(birth))
    except (ValueError, TypeError):
        raise CommerceError('Enter your date of birth.')
    today = now().date()
    years = today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    if years < 0 or years > 120:
        raise CommerceError('Enter a valid date of birth.')
    return 'blocked' if years < 13 else 'teen' if years < 18 else 'adult'

def paid_access(row, at=None):
    at = at or now()
    if not row or row.get('blocked') or row.get('refund_pending'):
        return False
    if row.get('status') not in {'active', 'past_due'}:
        return False
    until = dt(row.get('paid_until'))
    if until and until > at:
        return True
    grace = dt(row.get('grace_until'))
    return row.get('status') == 'past_due' and bool(grace and grace > at)

class Commerce:
    def __init__(self, app, db, verify, legacy, developer):
        self.app, self.db, self.verify, self.legacy, self.developer = app, db, verify, legacy, developer
        self.key = os.getenv('STRIPE_SECRET_KEY', '')
        self.env = 'live' if self.key.startswith('sk_live_') else 'test'
        self.site = os.getenv('CORNER_SITE_URL', '').rstrip('/')
        self.api = os.getenv('CORNER_API_URL', '').rstrip('/')
        self.secret = os.getenv('CORNER_COMMERCE_SECRET', '')
        self.enabled = os.getenv('CORNER_CHECKOUT_ENABLED', 'false').lower() == 'true'
        self.mail_enabled = os.getenv('CORNER_EMAIL_ENABLED', 'false').lower() == 'true'
        self.marketing_enabled = os.getenv('CORNER_MARKETING_ENABLED', 'false').lower() == 'true'
        self.prices = {'month': os.getenv('STRIPE_PRICE_MONTHLY', ''), 'year': os.getenv('STRIPE_PRICE_ANNUAL', '')}
        self.support = 'cornerboxers@gmail.com'
        self.register()

    def rows(self, table, **where):
        q = self.db.table(table).select('*')
        for key, val in where.items():
            q = q.eq(key, val)
        return q.execute().data or []

    def one(self, table, **where):
        rows = self.rows(table, **where)
        return rows[0] if rows else None

    def put(self, table, data):
        return self.db.table(table).upsert(data).execute()

    def update(self, table, data, **where):
        q = self.db.table(table).update(data)
        for key, val in where.items():
            q = q.eq(key, val)
        return q.execute()

    @contextmanager
    def lock(self, key):
        owner = secrets.token_hex(16)
        if not self.db.rpc('corner_lock', {'p_key': key, 'p_owner': owner, 'p_seconds': 300}).execute().data:
            raise CommerceError('Another request is finishing. Please try again shortly.', 409)
        try:
            yield
        finally:
            self.db.rpc('corner_unlock', {'p_key': key, 'p_owner': owner}).execute()

    def rate(self, key, limit=10, seconds=3600):
        digest = hashlib.sha256(key.encode()).hexdigest()
        if not self.db.rpc('corner_rate', {'p_key': digest, 'p_limit': limit, 'p_seconds': seconds}).execute().data:
            raise CommerceError('Please wait before trying again.', 429)

    def uid(self):
        uid = self.verify()
        if not uid:
            raise CommerceError('Sign in to continue.', 401)
        return str(uid)

    def account(self, uid):
        a = self.one('corner_accounts', user_id=uid)
        if not a:
            # Fetch the verified address through Auth admin, never trust a client email.
            u = self.db.auth.admin.get_user_by_id(uid).user
            if not u or not u.email:
                raise CommerceError('A verified email account is required.', 403)
            # Ignore duplicates safely: two initial page requests may arrive together.
            self.db.table('corner_accounts').upsert({'user_id': uid, 'email': u.email.lower(),
                'unsub_token': secrets.token_urlsafe(32)}, ignore_duplicates=True).execute()
            a = self.one('corner_accounts', user_id=uid)
        return a

    def ready(self, a):
        return bool(a and not a.get('deletion_pending') and a.get('age_band') in {'adult','teen'}
            and a.get('terms_version') == VERSION
            and (a['age_band'] == 'adult' or a.get('guardian_approved_at')))

    def require_ready(self, uid):
        a = self.account(uid)
        if not self.ready(a):
            raise CommerceError('Finish your account setup before continuing.', 403)
        return a

    def billing(self, uid):
        return self.one('corner_billing', user_id=uid, environment=self.env) or {'user_id': uid, 'environment': self.env, 'status': 'none'}

    def save_billing(self, row):
        self.put('corner_billing', {**row, 'updated_at': iso()})

    def effective_plan(self, uid):
        existing = self.legacy(uid)
        if not uid or existing.get('plan_tier') == 'pro':
            return existing
        b = self.billing(uid)
        if paid_access(b):
            until = max(filter(None, [dt(b.get('paid_until')), dt(b.get('grace_until'))]))
            return {'plan_tier': 'pro', 'source': 'stripe', 'pro_expires_at': iso(until), 'promo_code_id': None}
        return existing

    def stripe(self, method, path, data=None, idem=None):
        if not self.key:
            raise CommerceError('Billing is not configured yet.', 503)
        headers = {'Authorization': 'Bearer '+self.key, 'Stripe-Version': STRIPE_VERSION}
        if idem:
            headers['Idempotency-Key'] = 'corner:'+self.env+':'+idem
        try:
            r = requests.request(method, 'https://api.stripe.com/v1/'+path, headers=headers,
                params=data if method == 'GET' else None, data=data if method != 'GET' else None, timeout=(5, 15))
            payload = r.json()
        except (requests.RequestException, ValueError):
            raise CommerceError('The billing provider could not be reached. Please try again.', 503)
        if not r.ok:
            # Log only Stripe code/request ID; don't expose provider or customer details.
            self.app.logger.warning('stripe request failed code=%s request=%s', payload.get('error',{}).get('code'),r.headers.get('Request-Id'))
            raise CommerceError('The billing provider could not complete this request. Contact support if it persists.', 502)
        return payload

    def customer(self, uid, a):
        b = self.billing(uid)
        if not b.get('customer_id'):
            c = self.stripe('POST', 'customers', {'email': a.get('guardian_email') if a['age_band']=='teen' else a['email'],
                'metadata[corner_user_id]': uid}, 'customer:'+uid)
            b['customer_id'] = c['id']
            self.save_billing(b)
        return b

    def reconcile(self, uid):
        """Caller holds user lock. Retrieve current provider state, never apply old event snapshots."""
        b = self.billing(uid)
        if not b.get('customer_id'):
            return b
        subs = self.stripe('GET', 'subscriptions', {'customer': b['customer_id'], 'status':'all', 'limit':100})
        if subs.get('has_more'):
            raise CommerceError('Subscription history requires support review.',409)
        usable = [s for s in subs['data'] if s.get('metadata',{}).get('corner_user_id') == uid
                  and s['status'] not in TERMINAL]
        if len(usable) > 1:
            raise CommerceError('Multiple subscriptions need support review. No new charge has been started.', 409)
        s = usable[0] if usable else next((x for x in subs['data'] if x['id']==b.get('subscription_id')), None)
        if not s:
            return b
        invoice = None
        if s.get('latest_invoice'):
            invoice = self.stripe('GET', 'invoices/'+objid(s['latest_invoice']))
        b.update(subscription_id=s['id'], status=s['status'],
            period_end=iso(dt(s['current_period_end'])), cancel_at_period_end=bool(s['cancel_at_period_end']),
            price_id=s['items']['data'][0]['price']['id'])
        if invoice and invoice.get('paid') and not b.get('blocked') and not b.get('refund_pending'):
            # Restrict access to our configured recurring products.
            if b['price_id'] not in self.prices.values():
                raise CommerceError('This subscription needs support review.', 409)
            b['paid_until'] = iso(dt(s['current_period_end']))
            b['grace_until'] = None
        elif s['status']=='past_due' and b.get('paid_until'):
            # Stable deadline anchored to the last paid period, never extended by retries.
            b['grace_until'] = iso(dt(b['paid_until'])+timedelta(days=3))
        else:
            b['grace_until'] = None
        if invoice and invoice.get('paid') and invoice.get('amount_paid',0)>0 and not b.get('first_invoice_id'):
            # Find the FIRST ever paid invoice, not whichever webhook happened to arrive first.
            invs = self.stripe('GET','invoices',{'customer':b['customer_id'],'status':'paid','limit':100})
            if invs.get('has_more'):
                raise CommerceError('Billing history requires support review.',409)
            first = min((i for i in invs['data'] if objid(i.get('subscription')) in {sub['id'] for sub in subs['data'] if sub.get('metadata',{}).get('corner_user_id')==uid} and i.get('amount_paid',0)>0),
                        key=lambda i:i['created'],default=None)
            if first:
                b['first_invoice_id'] = first['id']
                b['first_payment_at'] = iso(dt(first['status_transitions']['paid_at']))
        sessions=self.stripe('GET','checkout/sessions',{'customer':b['customer_id'],'limit':100})
        if sessions.get('has_more'):
            raise CommerceError('Checkout history requires support review.',409)
        for session in sessions.get('data',[]):
            if session.get('status')=='complete' and objid(session.get('subscription'))==s['id']:
                self.consume_discount(uid,session)
        self.save_billing(b)
        if s['status'] in TERMINAL:
            self.enqueue('ended:'+s['id'],uid,'Your Corner subscription has ended',
                'Your paid subscription has ended. Learn, Train, your profile, and your earned progress are still available. Any separate valid promo access stays active.')
        if s.get('cancel_at_period_end'):
            self.enqueue('cancel:'+s['id']+':'+str(s['current_period_end']),uid,'Your Corner cancellation is confirmed',
                'Your subscription will not renew. Your paid access continues through '+dt(s['current_period_end']).strftime('%B %d, %Y')+'.')
        return b

    def enqueue(self, key, uid, subject, body, category='service', recipient=None):
        a = self.account(uid) if uid else None
        recipient = recipient or (a.get('guardian_email') if a and a['age_band']=='teen' and category=='service' else a.get('email') if a else None)
        if not recipient:
            return
        key = self.env+':'+key
        self.db.table('corner_mail').upsert({'id':key,'user_id':uid,'recipient':recipient,
            'subject':subject,'body':body,'category':category},ignore_duplicates=True).execute()

    def checkout(self, uid, interval, adult_confirmed=False):
        a = self.require_ready(uid)
        if not adult_confirmed:
            raise CommerceError('An adult purchaser must accept the subscription terms.')
        if not self.enabled:
            raise CommerceError('Checkout is not open yet.',503)
        if self.env=='live' and (os.getenv('CORNER_LIVE_READY')!='true' or not os.getenv('CORNER_LEGAL_NAME')):
            raise CommerceError('Live checkout has not been enabled.',503)
        if interval not in self.prices or not self.prices[interval]:
            raise CommerceError('Choose monthly or annual billing.')
        # Existing promo grants never silently become paid subscriptions.
        if self.legacy(uid).get('plan_tier')=='pro':
            raise CommerceError('You already have Pro access. You can subscribe after your current promo access ends.',409)
        b = self.customer(uid,a)
        b = self.reconcile(uid)
        if b.get('status') not in {'none','canceled','incomplete_expired'}:
            raise CommerceError('You already have a subscription. Use Manage billing.',409)
        if b.get('blocked'):
            raise CommerceError('Contact support before starting another subscription.',409)
        if b.get('refund_pending'):
            raise CommerceError('Your refund is still being processed.',409)
        # Reuse a still-open session rather than permit duplicate checkout windows.
        if b.get('checkout_id'):
            c = self.stripe('GET','checkout/sessions/'+b['checkout_id'])
            if c['status']=='open':
                return c['url']
            if c['status']=='complete' and b.get('status') not in TERMINAL:
                self.reconcile(uid)
                raise CommerceError('Your last checkout is being confirmed. Refresh billing shortly.',409)
        if b.get('checkout_payload') and dt(b.get('checkout_expires_at')) and dt(b['checkout_expires_at'])>now():
            if b.get('checkout_interval')!=interval:
                raise CommerceError('Retry the original billing interval while the previous request completes.',409)
            return self.finish_checkout(uid,b,b['checkout_payload'])
        price = self.stripe('GET','prices/'+self.prices[interval])
        expected = 999 if interval=='month' else 9900
        if (price.get('currency')!='usd' or price.get('unit_amount')!=expected or not price.get('active')
            or price.get('recurring',{}).get('interval')!=interval or price.get('recurring',{}).get('interval_count')!=1
            or bool(price.get('livemode')) != (self.env=='live')):
            raise CommerceError('The configured price needs administrator review.',503)
        nonce = b.get('checkout_nonce')
        if not nonce or b.get('checkout_id') or not dt(b.get('checkout_expires_at')) or dt(b['checkout_expires_at'])<=now():
            nonce = secrets.token_hex(16)
            b.update(checkout_nonce=nonce,checkout_id=None,checkout_interval=interval,checkout_payload=None,checkout_expires_at=iso(dt(int(time.time())+3600)))
            self.save_billing(b)
        if b.get('checkout_interval') != interval:
            raise CommerceError('Retry the original billing interval while the previous request completes.',409)
        data = {'mode':'subscription','customer':b['customer_id'], 'client_reference_id':uid,
            'payment_method_types[0]':'card',
            'line_items[0][price]':price['id'],'line_items[0][quantity]':1,
            'success_url':self.site+'/?corner_billing=success','cancel_url':self.site+'/?corner_billing=cancel',
            'subscription_data[metadata][corner_user_id]':uid,'metadata[corner_user_id]':uid,
            'metadata[corner_terms_version]':VERSION,'billing_address_collection':'required',
            'shipping_address_collection[allowed_countries][0]':'US',
            'consent_collection[terms_of_service]':'required',
            'custom_text[submit][message]':'Automatically renews until canceled. Cancel in Corner > Billing. First paid purchase: request a refund within 7 days.',
            'expires_at':int(dt(b['checkout_expires_at']).timestamp())}
        if os.getenv('STRIPE_AUTOMATIC_TAX')=='true':
            data.update({'automatic_tax[enabled]':'true','customer_update[address]':'auto','customer_update[shipping]':'auto'})
        # Use one already redeemed, unused Corner discount. Never trust a client amount/coupon.
        reds = self.db.table('promo_redemptions').select('*').eq('user_id',uid).is_('checkout_consumed_at','null').order('redeemed_at').execute().data or []
        chosen = None
        for red in reds:
            if red.get('benefit_type') not in {'discount_percent','discount_fixed_cents'}:
                continue
            claim = self.one('corner_discount_claims',redemption_id=red['id'],environment=self.env)
            if claim and claim.get('consumed_at'):
                continue
            coupon = red.get('stripe_coupon_id')
            if not coupon:
                # Existing simple Corner discount semantics: once, on the first invoice.
                amount = int(red.get('benefit_value') or 0)
                if amount<=0 or (red['benefit_type']=='discount_percent' and amount>100):
                    raise CommerceError('This saved discount needs support review.',409)
                fields={'duration':'once','name':'Corner promo','metadata[corner_redemption_id]':red['id']}
                fields.update({'percent_off':amount} if red['benefit_type']=='discount_percent' else {'amount_off':amount,'currency':'usd'})
                coupon=self.stripe('POST','coupons',fields,'coupon:'+red['id'])['id']
            cp=self.stripe('GET','coupons/'+coupon)
            if not cp.get('valid'):
                raise CommerceError('Your saved discount needs support review before checkout.',409)
            data['discounts[0][coupon]']=coupon
            data['metadata[corner_redemption_id]']=red['id']
            chosen=red['id']
            break
        b['checkout_payload']=data
        self.save_billing(b)
        return self.finish_checkout(uid,b,data)

    def finish_checkout(self,uid,b,data):
        c=self.stripe('POST','checkout/sessions',data,'checkout:'+uid+':'+b['checkout_nonce'])
        b.update(checkout_id=c['id'],checkout_expires_at=iso(dt(c['expires_at'])))
        self.save_billing(b)
        chosen=data.get('metadata[corner_redemption_id]')
        if chosen:
            self.db.table('corner_discount_claims').upsert({'redemption_id':chosen,'user_id':uid,
                'environment':self.env,'checkout_id':c['id']},ignore_duplicates=True).execute()
        self.audit(uid,'checkout_'+b['checkout_interval'])
        return c['url']

    def consume_discount(self,uid,session):
        rid=session.get('metadata',{}).get('corner_redemption_id')
        if not rid or session.get('payment_status') not in {'paid','no_payment_required'}:
            return
        red=self.one('promo_redemptions',id=rid,user_id=uid)
        if not red:
            return
        self.put('corner_discount_claims',{'redemption_id':rid,'user_id':uid,
            'environment':self.env,'checkout_id':session['id'],'consumed_at':iso()})
        if self.env=='live':
            self.update('promo_redemptions',{'checkout_consumed_at':iso()},id=rid,user_id=uid)

    def refund(self,uid):
        b=self.reconcile(uid)
        if b.get('refund_id'):
            return 'Your refund has already been requested.'
        paid=dt(b.get('first_payment_at'))
        if not paid or (now()>paid+timedelta(days=7) and not b.get('refund_pending')):
            raise CommerceError('The automatic first-purchase refund window has ended. Contact '+self.support+' for help.',409)
        inv=self.stripe('GET','invoices/'+b['first_invoice_id'])
        if objid(inv.get('subscription'))!=b.get('subscription_id') or not inv.get('payment_intent'):
            raise CommerceError('This refund needs support review.',409)
        pi=self.stripe('GET','payment_intents/'+objid(inv['payment_intent']))
        charge=self.stripe('GET','charges/'+objid(pi['latest_charge']))
        if charge.get('disputed'):
            raise CommerceError('This payment has a dispute. Contact support.',409)
        previous = None
        if charge.get('amount_refunded',0)>0:
            refunds=self.stripe('GET','refunds',{'payment_intent':pi['id'],'limit':100})
            previous=next((r for r in refunds['data'] if r.get('metadata',{}).get('corner_invoice')==b['first_invoice_id']),None)
            if not previous:
                raise CommerceError('This payment already has a refund. Contact support.',409)
        b['refund_pending']=True
        self.save_billing(b)
        # Cancel FIRST: prevents charging again if a later refund request fails; retries use the same idempotency key.
        if b['status'] not in TERMINAL:
            self.stripe('DELETE','subscriptions/'+b['subscription_id'],{'prorate':'false','invoice_now':'false'},'refund-cancel:'+b['subscription_id'])
        refund=previous or self.stripe('POST','refunds',{'payment_intent':pi['id'],'reason':'requested_by_customer','metadata[corner_invoice]':b['first_invoice_id']},'refund:'+b['first_invoice_id'])
        b.update(refund_id=refund['id'],blocked=True,status='canceled',grace_until=None,paid_until=None,refund_pending=False)
        self.save_billing(b)
        self.enqueue('refund:'+refund['id'],uid,'Your Corner refund has been requested',
            'We requested a refund of your first subscription payment. Your subscription is canceled. Your bank determines when the credit appears. If the refund cannot be completed, contact '+self.support+'.')
        return 'Refund requested and subscription canceled. Your Free account and earned progress remain.'

    def audit(self,uid,action):
        self.db.table('corner_consent_log').insert({'user_id':uid,'action':action,'version':VERSION}).execute()

    def process_event(self,event):
        typ=event['type']; ob=event['data']['object']
        customer=objid(ob.get('customer'))
        if typ.startswith('refund.') and not customer:
            charge_id=objid(ob.get('charge'))
            if not charge_id and ob.get('payment_intent'):
                pi=self.stripe('GET','payment_intents/'+objid(ob['payment_intent']))
                charge_id=objid(pi.get('latest_charge'))
            if charge_id:
                customer=objid(self.stripe('GET','charges/'+charge_id).get('customer'))
        if typ.startswith('charge.dispute.'):
            charge=self.stripe('GET','charges/'+objid(ob['charge']))
            customer=objid(charge.get('customer'))
        row=self.one('corner_billing',customer_id=customer) if customer else None
        if not row or row['environment']!=self.env:
            return
        uid=row['user_id']
        with self.lock('billing:'+self.env+':'+uid):
            if typ in {'charge.refunded','charge.dispute.created','charge.dispute.closed'}:
                # Fail closed on a full refund or dispute. Admin can review; no automatic regrant after dispute.
                ch=ob if typ=='charge.refunded' else charge
                if ch.get('disputed') or ch.get('refunded'):
                    row.update(blocked=True,paid_until=None,grace_until=None)
                    self.save_billing(row)
                    if row.get('subscription_id') and row['status'] not in TERMINAL:
                        self.stripe('DELETE','subscriptions/'+row['subscription_id'],{'prorate':'false','invoice_now':'false'},'risk-cancel:'+row['subscription_id'])
            if typ=='checkout.session.completed':
                current=self.stripe('GET','checkout/sessions/'+ob['id'])
                self.consume_discount(uid,current)
            b=self.reconcile(uid)
            if typ in {'invoice.paid','invoice.payment_failed','invoice.payment_action_required'}:
                inv=self.stripe('GET','invoices/'+ob['id'])
                if inv.get('subscription') and objid(inv['subscription'])==b.get('subscription_id'):
                    if inv.get('paid'):
                        self.enqueue('paid:'+inv['id'],uid,'Your Corner payment is confirmed',
                            'We received $'+format(inv.get('amount_paid',0)/100,'.2f')+' USD. Your subscription details and invoices are available in Corner > Billing. Thanks for training with us.')
                    elif typ!='invoice.paid':
                        self.enqueue('failed:'+inv['id'],uid,'Your Corner payment needs attention',
                            'We could not complete your payment. Open Corner > Billing > Manage billing to update your payment method. '
                            +('Your paid-access grace period ends '+dt(b['grace_until']).strftime('%B %d, %Y')+'.' if b.get('grace_until') else 'Pro unlocks after payment is confirmed.'))
            if typ.startswith('refund.') and ob.get('status') in {'failed','canceled'}:
                self.enqueue('refund-problem:'+ob['id'],uid,'Your Corner refund needs attention',
                    'The payment provider could not complete your refund. Your subscription remains canceled. Please contact '+self.support+' so we can resolve this.')

    def webhook(self):
        body=request.get_data(cache=True)
        if len(body)>1024*1024:
            raise CommerceError('Payload too large.',413)
        secret=os.getenv('STRIPE_WEBHOOK_SECRET','')
        parts={}
        for p in request.headers.get('Stripe-Signature','').split(','):
            k,_,v=p.partition('=');parts.setdefault(k,[]).append(v)
        try:
            stamp=int(parts['t'][0])
        except (KeyError,ValueError,IndexError):
            raise CommerceError('Invalid signature.',400)
        expected=hmac.new(secret.encode(),str(stamp).encode()+b'.'+body,hashlib.sha256).hexdigest()
        if not secret or abs(time.time()-stamp)>300 or not any(hmac.compare_digest(expected,v) for v in parts.get('v1',[])):
            raise CommerceError('Invalid signature.',400)
        event=json.loads(body)
        if bool(event.get('livemode'))!=(self.env=='live'):
            raise CommerceError('Wrong billing environment.',400)
        self.db.table('corner_events').upsert({'id':event['id'],'environment':self.env,'payload':event},ignore_duplicates=True).execute()
        with self.lock('event:'+event['id']):
            stored=self.one('corner_events',id=event['id'])
            if not stored.get('processed_at'):
                self.process_event(event)
                self.update('corner_events',{'processed_at':iso(),'error':None},id=event['id'])
        return {'received':True}

    def send_mail(self,row):
        if not row['id'].startswith(self.env+':'):
            return
        a=self.one('corner_accounts',user_id=row['user_id']) if row.get('user_id') else None
        category=row['category']
        if row['id'].startswith(self.env+':failed:'):
            inv=self.stripe('GET','invoices/'+row['id'].split(':',2)[2])
            if inv.get('paid') or inv.get('status') in {'void','uncollectible'}:
                self.update('corner_mail',{'status':'suppressed'},id=row['id']);return
        if row['id'].startswith(self.env+':cancel:'):
            b=self.billing(row['user_id'])
            if not b.get('cancel_at_period_end') or b['status'] in TERMINAL:
                self.update('corner_mail',{'status':'suppressed'},id=row['id']);return
        allowed=not a or not a.get('suppressed')
        if a and a.get('deletion_pending'):
            allowed=False
        if category in {'marketing','training'}:
            allowed=allowed and bool(a and not a.get('deletion_pending') and a['age_band']=='adult' and a.get('marketing' if category=='marketing' else 'training_emails') and self.marketing_enabled and os.getenv('CORNER_POSTAL_ADDRESS'))
        if not allowed:
            self.update('corner_mail',{'status':'suppressed'},id=row['id']);return
        # Explicit allow-list for tests, so tests cannot email arbitrary customers.
        if self.env=='test':
            test={e.strip().lower() for e in os.getenv('CORNER_TEST_EMAILS','').split(',') if e.strip()}
            if row['recipient'].lower() not in test:
                self.update('corner_mail',{'status':'suppressed'},id=row['id']);return
        try:
            self.rate('mail-budget:'+now().strftime('%Y-%m-%d'),200,90000)
        except CommerceError:
            return
        self.update('corner_mail',{'status':'sending','attempts':row['attempts']+1},id=row['id'])
        footer=os.getenv('CORNER_LEGAL_NAME','Corner Boxing')+' / Corner Boxing | Support: '+self.support
        headers={'IdempotencyKey':hashlib.sha256(row['id'].encode()).hexdigest()}
        if category in {'marketing','training'}:
            url=self.api+'/commerce/unsubscribe?'+urlencode({'token':a['unsub_token']})
            footer+='\nOptional promotional message from Corner Boxing.\n'+os.getenv('CORNER_POSTAL_ADDRESS','')+'\nUnsubscribe: '+url
            headers.update({'List-Unsubscribe':'<'+url+'>','List-Unsubscribe-Post':'List-Unsubscribe=One-Click'})
        body=row['body']+'\n\n'+footer
        try:
            r=requests.post('https://api.brevo.com/v3/smtp/email',headers={'api-key':os.getenv('BREVO_API_KEY',''),'Content-Type':'application/json'},
                json={'sender':{'name':'Corner Boxing' if category in {'service','guardian'} else 'Coach at Corner','email':os.getenv('CORNER_FROM_EMAIL','')},
                      'replyTo':{'email':self.support},'to':[{'email':row['recipient']}], 'subject':row['subject'],
                      'textContent':body,'headers':headers},timeout=(5,12))
        except requests.RequestException:
            # Unknown delivery outcome: DO NOT auto-resend after provider deduplication window.
            self.update('corner_mail',{'status':'uncertain','error':'Network outcome unknown; check Brevo logs before retry.'},id=row['id']);return
        if r.status_code==429:
            self.update('corner_mail',{'status':'pending','error':'Provider daily/rate limit reached.'},id=row['id']);return
        if not r.ok:
            self.update('corner_mail',{'status':'failed','error':'Provider HTTP '+str(r.status_code)},id=row['id']);return
        self.update('corner_mail',{'status':'sent','provider_id':r.json().get('messageId'),'sent_at':iso(),'error':None},id=row['id'])

    def scheduled_messages(self):
        # One bounded page per run; cursor prevents a small group starving later accounts.
        task=self.one('corner_tasks',key='reminders:'+self.env) or {}
        q=self.db.table('corner_accounts').select('*').eq('deletion_pending',False).order('user_id').limit(5)
        if task.get('cursor_id'):q=q.gt('user_id',task['cursor_id'])
        accounts=q.execute().data or []
        for a in accounts:
            uid=a['user_id']
            if not self.ready(a):continue
            p=self.legacy(uid);end=dt(p.get('pro_expires_at'))
            if p.get('source')=='promo' and end and now()<end<=now()+timedelta(days=3):
                self.enqueue('promo-ending:'+uid+':'+str(p.get('promo_code_id'))+':'+iso(end),uid,
                    'Your free Corner Pro access ends soon','Your current promo access ends on '+end.strftime('%B %d, %Y')+'. You will not be charged automatically. Learn, Train, and your earned progress remain available on Free.')
            b=self.billing(uid);end=dt(b.get('period_end'))
            if b['status']=='active' and end and not b.get('cancel_at_period_end'):
                days=(end-now()).total_seconds()/86400
                windows=[30,7,3] if b.get('price_id')==self.prices['year'] else [7,3]
                for window in windows:
                    if window-1<days<=window:
                        self.enqueue('reminder:'+b['subscription_id']+':'+iso(end)+':'+str(window),uid,
                            'Your Corner renewal is coming up','Your '+('annual' if b.get('price_id')==self.prices['year'] else 'monthly')+' plan renews on '+end.strftime('%B %d, %Y')+'. Review your upcoming charge, discounts, and applicable taxes in Corner > Billing > Manage billing. You can cancel there before renewal.')
            if self.marketing_enabled and a['training_emails'] and a['age_band']=='adult':
                times=[dt(a['created_at'])]
                for table,col,filters in [('class_sessions','listened_complete_at',{'listened_complete':True}),
                    ('train_sessions','completed_at',{'status':'completed'}),('learn_sessions','completed_at',{'status':'completed'})]:
                    q=self.db.table(table).select(col).eq('user_id',uid).not_.is_(col,'null').order(col,desc=True).limit(1)
                    for k,v in filters.items():q=q.eq(k,v)
                    rows=q.execute().data or []
                    if rows and rows[0].get(col):times.append(dt(rows[0][col]))
                if max(times)<now()-timedelta(days=14):
                    week=now().strftime('%G-%V')
                    self.enqueue('train:'+uid+':'+week,uid,'A short session is there when you want it',
                        'Hey — if you want to get a few rounds in this week, pick a Train mode and start with a pace that feels manageable. Your saved progress is still there.\n\nOpen Corner: '+self.site+'\n\n— Coach at Corner','training')
        self.put('corner_tasks',{'key':'reminders:'+self.env,'cursor_id':accounts[-1]['user_id'] if len(accounts)==5 else None,'updated_at':iso()})

    def campaign_page(self):
        if not self.marketing_enabled or not os.getenv('CORNER_POSTAL_ADDRESS'):return
        campaigns=self.db.table('corner_campaigns').select('*').eq('environment',self.env).eq('state','sending').order('created_at').limit(1).execute().data or []
        if not campaigns:return
        c=campaigns[0]
        q=self.db.table('corner_accounts').select('*').eq('marketing',True).eq('age_band','adult').eq('suppressed',False).eq('deletion_pending',False).lte('created_at',c['sent_at']).order('user_id').limit(25)
        if c.get('cursor_id'):q=q.gt('user_id',c['cursor_id'])
        rows=q.execute().data or []
        for a in rows:
            self.enqueue('campaign:'+c['id']+':'+a['user_id'],a['user_id'],c['subject'],c['body'],'marketing')
        self.update('corner_campaigns',{'cursor_id':rows[-1]['user_id'] if rows else c.get('cursor_id'),
            'state':'queued' if len(rows)<25 else 'sending'},id=c['id'])

    def maintenance(self, task='mail'):
        # Calls are independently scheduled; never rely on someone opening Corner.
        deadline=time.monotonic()+20
        with self.lock('maintenance:'+self.env+':'+task):
            events=self.db.table('corner_events').select('*').eq('environment',self.env).is_('processed_at','null').lte('next_attempt_at',iso()).order('created_at').limit(1).execute().data or [] if task=='events' else []
            for event in events:
                if time.monotonic()>deadline:break
                try:
                    with self.lock('event:'+event['id']):
                        if not self.one('corner_events',id=event['id']).get('processed_at'):
                            self.process_event(event['payload'])
                            self.update('corner_events',{'processed_at':iso(),'error':None},id=event['id'])
                except Exception:
                    self.update('corner_events',{'attempts':event['attempts']+1,'next_attempt_at':iso(now()+timedelta(seconds=min(3600,30*(2**min(event['attempts'],7))))),'error':'Processing failed; review API logs.'},id=event['id'])
            bills=self.db.table('corner_billing').select('*').eq('environment',self.env).not_.is_('customer_id','null').lt('updated_at',iso(now()-timedelta(hours=6))).order('updated_at').limit(1).execute().data or [] if task=='billing' else []
            for b in bills:
                if time.monotonic()>deadline:break
                try:
                    with self.lock('billing:'+self.env+':'+b['user_id']):self.reconcile(b['user_id'])
                except Exception:self.app.logger.exception('Billing reconciliation failed')
            if task=='reminders':self.scheduled_messages()
            if task=='campaigns':self.campaign_page()
            if task=='mail' and self.mail_enabled and time.monotonic()<deadline:
                # Stale sending rows stay uncertain, never duplicate blindly.
                self.db.table('corner_mail').update({'status':'uncertain','error':'Interrupted send; review provider logs.'}).like('id',self.env+':%').eq('status','sending').lt('created_at',iso(now()-timedelta(minutes=10))).execute()
                mails=self.db.table('corner_mail').select('*').like('id',self.env+':%').eq('status','pending').in_('category',['service','guardian']).order('created_at').limit(5).execute().data or []
                if len(mails)<5:
                    mails += self.db.table('corner_mail').select('*').like('id',self.env+':%').eq('status','pending').in_('category',['training','marketing']).order('created_at').limit(5-len(mails)).execute().data or []
                for m in mails:
                    if time.monotonic()>deadline:break
                    self.send_mail(m)
            self.db.table('corner_signup_tickets').delete().lt('expires_at',iso()).execute()
            self.db.table('corner_limits').delete().lt('expires_at',iso(now()-timedelta(days=1))).execute()
            self.db.table('corner_events').delete().not_.is_('processed_at','null').lt('created_at',iso(now()-timedelta(days=30))).execute()
            self.db.table('corner_mail').delete().in_('status',['sent','suppressed']).lt('created_at',iso(now()-timedelta(days=90))).execute()
        return {'ok':True}

    def public_page(self,title,body):
        return Response('<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="referrer" content="no-referrer"><title>'+html.escape(title)+'</title><style>body{font:17px/1.6 system-ui;background:#111;color:#eee;max-width:740px;margin:50px auto;padding:20px}a{color:#ff7a80}button,input,select{font:inherit;padding:12px;margin:8px 0}label{display:block}button{cursor:pointer}</style><h1>'+html.escape(title)+'</h1>'+body+'</html>',mimetype='text/html',headers={'Cache-Control':'no-store','Referrer-Policy':'no-referrer','X-Content-Type-Options':'nosniff','Content-Security-Policy':"default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'"})

    def register(self):
        app=self.app
        def route(path,methods=['GET']):
            def decorate(fn):
                def guarded(**kwargs):
                    try:
                        result=fn(**kwargs)
                        if isinstance(result,dict):
                            response=jsonify(result)
                            response.headers['Cache-Control']='no-store'
                            return response
                        return result
                    except CommerceError as e:return jsonify({'error':str(e)}),e.status
                    except Exception:
                        app.logger.exception('Corner commerce request failed')
                        return jsonify({'error':'This request could not be completed. Please retry or contact support.'}),503
                guarded.__name__='commerce_'+fn.__name__
                app.route('/commerce'+path,methods=methods)(guarded)
                return fn
            return decorate

        @app.before_request
        def corner_account_gate():
            if request.method=='OPTIONS' or request.path.startswith('/commerce') or request.path in {'/','/_whoami','/me'}:
                return None
            uid=self.verify()
            if uid:
                try:
                    if not self.ready(self.account(str(uid))):
                        return jsonify({'error':'Finish your Corner account setup.','code':'account_setup_required'}),403
                except Exception:
                    return jsonify({'error':'Account access could not be verified. Try again shortly.'}),503

        @route('/config')
        def config():
            return {'mode':self.env,'checkout_enabled':self.enabled,'version':VERSION,'support':self.support,
                'monthly':999,'annual':9900,'legal_url':self.api+'/commerce/legal/terms'}

        @route('/signup', ['POST'])
        def signup():
            data=request.get_json(silent=True) or {}
            self.rate('signup:'+str(request.remote_addr),120)
            band=age_band(data.get('birth_date'))
            if band=='blocked':raise CommerceError('We cannot create this account.',403)
            email=str(data.get('email','')).strip().lower()
            if not re.fullmatch(r'[^\s@]{1,64}@[^\s@.]+(?:\.[^\s@.]+)+',email) or len(email)>254:
                raise CommerceError('Enter a valid email address.')
            self.rate('signup-email:'+email,3)
            tok=secrets.token_urlsafe(32)
            self.put('corner_signup_tickets',{'token':tok,'email':email,'age_band':band,'expires_at':iso(now()+timedelta(minutes=5))})
            r=requests.post(os.getenv('SUPABASE_URL','')+'/auth/v1/otp',
                headers={'apikey':os.getenv('SUPABASE_SERVICE_KEY',''),'Content-Type':'application/json'},
                json={'email':email,'create_user':True,'data':{'corner_ticket':tok}},
                params={'redirect_to':self.site+'/'},timeout=15)
            if not r.ok:raise CommerceError('Could not send the signup link. Try again later or contact support.',503)
            return {'message':'Check your email for your Corner sign-in link.'}

        @route('/account')
        def get_account():
            uid=self.uid();a=self.account(uid)
            self.update('corner_accounts',{'last_seen_at':iso()},user_id=uid)
            return {'ready':self.ready(a),'age_band':a.get('age_band'),'accepted':a.get('terms_version')==VERSION,
                'guardian_pending':a.get('age_band')=='teen' and not a.get('guardian_approved_at'),
                'marketing':a['marketing'],'training_emails':a['training_emails'],'deletion_pending':a['deletion_pending']}

        @route('/account', ['POST'])
        def setup_account():
            uid=self.uid();data=request.get_json(silent=True) or {}
            with self.lock('account:'+uid):
                a=self.account(uid)
                if a.get('deletion_pending'):raise CommerceError('Account deletion is pending.',403)
                band=a.get('age_band') or age_band(data.get('birth_date'))
                if band=='blocked':
                    self.update('corner_accounts',{'age_band':'blocked','deletion_pending':True},user_id=uid)
                    raise CommerceError('This account cannot continue. Contact support to remove the account.',403)
                if data.get('accept') is not True:raise CommerceError('Accept the Terms and acknowledge the Privacy and fitness notices.')
                update={'age_band':band,'terms_version':VERSION,'accepted_at':iso()}
                if band=='teen' and not a.get('guardian_approved_at'):
                    addr=str(data.get('guardian_email','')).strip().lower()
                    if not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+',addr) or addr==a['email']:
                        raise CommerceError('Enter a parent or guardian email different from your own.')
                    self.rate('guardian:'+uid,3,86400)
                    tok=secrets.token_urlsafe(32)
                    update.update(guardian_email=addr,guardian_token=tok,guardian_expires_at=iso(now()+timedelta(days=2)))
                    self.update('corner_accounts',update,user_id=uid)
                    self.enqueue('guardian:'+tok,uid,'Review a Corner Boxing account request',
                        'Someone using '+a['email']+' has asked you to approve their use of Corner Boxing. Corner includes boxing workouts and social features. Review the request here:\n'+self.api+'/commerce/guardian?'+urlencode({'token':tok})+'\n\nThis link expires in two days. If you do not recognize the request, ignore it. No payment will be taken.',
                        'guardian',addr)
                else:self.update('corner_accounts',update,user_id=uid)
                self.audit(uid,'terms_accepted')
                self.enqueue('welcome:'+uid,uid,'Welcome to Corner',
                    'Your Corner account is ready for setup. Learn the fundamentals, choose a Train mode, or try the demo class. Your workouts and account settings are at '+self.site+'. Questions? Reply to this email.')
            return {'ok':True,'ready':self.ready(self.account(uid))}

        @route('/preferences',['POST'])
        def preferences():
            uid=self.uid();a=self.require_ready(uid);d=request.get_json(silent=True) or {}
            marketing=d.get('marketing') is True;training=d.get('training_emails') is True
            if a['age_band']!='adult' and (marketing or training):raise CommerceError('Optional emails are currently available to adult accounts only.')
            self.update('corner_accounts',{'marketing':marketing,'training_emails':training,
                'consent_at':iso(),'consent_version':VERSION},user_id=uid)
            self.audit(uid,'email_preferences:'+str(marketing)+':'+str(training))
            return {'ok':True}

        @route('/guardian',['GET','POST'])
        def guardian():
            token=request.values.get('token','')
            a=self.one('corner_accounts',guardian_token=token) if len(token)>20 else None
            if not a or not dt(a.get('guardian_expires_at')) or dt(a['guardian_expires_at'])<now():
                raise CommerceError('This link has expired. Request a new link from Corner.',410)
            uid=a['user_id']
            if request.method=='POST':
                if request.form.get('adult')!='yes':raise CommerceError('An adult parent or guardian must confirm.')
                action=request.form.get('action','approve')
                with self.lock('billing:'+self.env+':'+uid):
                    if action=='approve':
                        self.update('corner_accounts',{'guardian_approved_at':iso()},user_id=uid)
                        self.audit(uid,'guardian_email_approval')
                    elif action=='checkout':
                        return redirect(self.checkout(uid,request.form.get('interval'),True),303)
                    elif action=='portal':
                        b=self.billing(uid)
                        if not b.get('customer_id'):raise CommerceError('No billing account exists yet.')
                        portal=self.stripe('POST','billing_portal/sessions',{'customer':b['customer_id'],'return_url':self.site+'/?corner_billing=return'})
                        return redirect(portal['url'],303)
                    elif action=='refund':
                        msg=self.refund(uid)
                        return self.public_page('Refund request',html.escape(msg))
                    elif action=='revoke':
                        b=self.billing(uid)
                        if b.get('subscription_id') and b['status'] not in TERMINAL:
                            self.stripe('DELETE','subscriptions/'+b['subscription_id'],{'prorate':'false','invoice_now':'false'},'guardian-revoke:'+b['subscription_id'])
                        self.update('corner_accounts',{'guardian_approved_at':None,'deletion_pending':True},user_id=uid)
                        self.audit(uid,'guardian_revoked')
                        return self.public_page('Permission withdrawn','<p>Access is disabled and subscription renewal is stopped. Contact '+self.support+' for data deletion or refund assistance.</p>')
                return self.public_page('Permission recorded','<p>The teen can return to Corner and refresh account setup. No purchase was made.</p>')
            action=request.args.get('purpose','approve')
            if action not in {'approve','checkout','portal','refund','revoke'}:action='approve'
            options='<label>Plan <select name="interval"><option value="month">$9.99 each month</option><option value="year">$99 each year</option></select></label>' if action=='checkout' else ''
            return self.public_page('Corner parent / guardian',
                '<p>Account: '+html.escape(a['email'])+'</p><p>Corner offers exercise audio, public profiles, gyms, posts, and comments. Review the <a href="'+self.api+'/commerce/legal/terms">Terms</a> and <a href="'+self.api+'/commerce/legal/privacy">Privacy Policy</a>. Fitness activity carries injury risk. This is not medical care or supervised coaching.</p>'
                '<p>Paid subscriptions renew automatically until canceled. Monthly: $9.99. Annual: $99. Applicable tax is shown at checkout. A first-purchase refund can be requested within seven days. Free participation does not require payment.</p>'
                '<form method="post"><input type="hidden" name="token" value="'+html.escape(token)+'"><input type="hidden" name="action" value="'+action+'">'+options+
                '<label><input type="checkbox" name="adult" value="yes" required> I am 18 or older and this teen’s parent or legal guardian. I have reviewed the policies and authorize this action.</label><button>'+html.escape({'approve':'Approve free account','checkout':'Continue to secure checkout','portal':'Manage billing','refund':'Request first-payment refund','revoke':'Withdraw permission and stop renewal'}[action])+'</button></form>')

        @route('/guardian-link',['POST'])
        def guardian_link():
            uid=self.uid();a=self.require_ready(uid)
            if a['age_band']!='teen':raise CommerceError('This account does not need a guardian link.')
            purpose=(request.get_json(silent=True) or {}).get('purpose','checkout')
            if purpose not in {'checkout','portal','refund','revoke'}:raise CommerceError('Invalid action.')
            self.rate('parent-billing:'+uid,3,3600)
            tok=secrets.token_urlsafe(32)
            self.update('corner_accounts',{'guardian_token':tok,'guardian_expires_at':iso(now()+timedelta(hours=24))},user_id=uid)
            self.enqueue('parent-billing:'+tok,uid,'Corner billing request for your teen',
                'Your teen requested a billing action. Review it here:\n'+self.api+'/commerce/guardian?'+urlencode({'token':tok,'purpose':purpose})+'\nThis link expires in 24 hours. No purchase is made unless you confirm checkout.', 'guardian',a['guardian_email'])
            return {'message':'A secure link has been sent to your parent or guardian.'}

        @route('/billing')
        def billing_status():
            uid=self.uid();a=self.account(uid);b=self.billing(uid)
            # No customer, subscription, tokens, or provider secrets in browser payload.
            return {'mode':self.env,'status':b['status'],'paid_until':b.get('paid_until'),
                'period_end':b.get('period_end'),'cancel_at_period_end':b.get('cancel_at_period_end',False),
                'grace_until':b.get('grace_until'),'has_customer':bool(b.get('customer_id')),
                'refund_requested':bool(b.get('refund_id')),'age_band':a.get('age_band'),
                'plan':self.effective_plan(uid),'checkout_enabled':self.enabled}

        @route('/refresh',['POST'])
        def refresh():
            uid=self.uid();self.rate('refresh:'+uid,12,3600)
            with self.lock('billing:'+self.env+':'+uid):self.reconcile(uid)
            return {'ok':True}

        @route('/checkout',['POST'])
        def checkout_route():
            uid=self.uid();a=self.require_ready(uid);d=request.get_json(silent=True) or {}
            if a['age_band']=='teen':raise CommerceError('Ask your parent or guardian to complete checkout.',403)
            self.rate('checkout:'+uid,10,3600)
            with self.lock('billing:'+self.env+':'+uid):url=self.checkout(uid,d.get('interval'),d.get('adult_confirmed') is True)
            return {'url':url}

        @route('/portal',['POST'])
        def portal():
            uid=self.uid();a=self.require_ready(uid)
            if a['age_band']=='teen':raise CommerceError('Ask your parent or guardian to manage billing.',403)
            self.rate('portal:'+uid,12,3600);b=self.billing(uid)
            if not b.get('customer_id'):raise CommerceError('You do not have a paid billing account yet.')
            p=self.stripe('POST','billing_portal/sessions',{'customer':b['customer_id'],'return_url':self.site+'/?corner_billing=return'})
            return {'url':p['url']}

        @route('/refund',['POST'])
        def refund_route():
            uid=self.uid();a=self.require_ready(uid)
            if a['age_band']=='teen':raise CommerceError('Ask your parent or guardian to request the refund.',403)
            if (request.get_json(silent=True) or {}).get('confirm') is not True:raise CommerceError('Confirm the refund and cancellation.')
            with self.lock('billing:'+self.env+':'+uid):message=self.refund(uid)
            return {'message':message}

        @route('/stripe-webhook',['POST'])
        def stripe_hook():return self.webhook()

        @route('/maintenance',['POST'])
        def maintenance_route():
            token=request.headers.get('Authorization','').removeprefix('Bearer ')
            if not self.secret or not hmac.compare_digest(token,self.secret):raise CommerceError('Unauthorized.',401)
            task=request.args.get('task','mail')
            if task not in {'mail','events','billing','reminders','campaigns'}:raise CommerceError('Unknown scheduled task.')
            return self.maintenance(task)

        @route('/unsubscribe',['GET','POST'])
        def unsubscribe():
            token=request.values.get('token','')
            a=self.one('corner_accounts',unsub_token=token) if len(token)>20 else None
            if not a:raise CommerceError('Invalid link.',400)
            if request.method=='POST':
                self.update('corner_accounts',{'marketing':False,'training_emails':False},user_id=a['user_id'])
                self.audit(a['user_id'],'email_unsubscribe')
                return self.public_page('You’re unsubscribed','<p>Optional marketing and training emails are off. Essential account and billing messages remain enabled.</p>')
            return self.public_page('Email preferences','<form method="post"><input type="hidden" name="token" value="'+html.escape(token)+'"><p>Stop optional marketing and training reminders?</p><button>Unsubscribe</button></form>')

        @route('/brevo-webhook',['POST'])
        def brevo_hook():
            tok=request.headers.get('Authorization','').removeprefix('Bearer ')
            webhook_secret=os.getenv('CORNER_EMAIL_WEBHOOK_SECRET','')
            if not webhook_secret or not hmac.compare_digest(tok,webhook_secret):raise CommerceError('Unauthorized.',401)
            event=request.get_json(silent=True) or {}
            addr=str(event.get('email','')).strip().lower()
            kind=event.get('event')
            for a in self.rows('corner_accounts',email=addr)+self.rows('corner_accounts',guardian_email=addr):
                if kind in {'unsubscribed','spam','hard_bounce','hardBounce','blocked','invalid','invalid_email'}:
                    upd={'marketing':False,'training_emails':False}
                    if kind!='unsubscribed':upd['suppressed']=True
                    self.update('corner_accounts',upd,user_id=a['user_id'])
            return {'ok':True}

        @route('/admin/health')
        def admin_health():
            uid=self.uid()
            if not self.developer(uid):raise CommerceError('Forbidden.',403)
            results={}
            for table,column,value in [('corner_mail','status','uncertain'),('corner_mail','status','failed'),('corner_events','processed_at',None)]:
                q=self.db.table(table).select('id',count='exact')
                q=q.is_(column,'null') if value is None else q.eq(column,value)
                results[table+':'+str(value)]=q.limit(1).execute().count
            return {'mode':self.env,'checkout_enabled':self.enabled,'email_enabled':self.mail_enabled,'counts':results}

        @route('/admin/campaigns',['GET','POST'])
        def campaigns():
            uid=self.uid()
            if not self.developer(uid):raise CommerceError('Forbidden.',403)
            if request.method=='GET':
                rows=self.db.table('corner_campaigns').select('*').eq('environment',self.env).order('created_at',desc=True).limit(20).execute().data or []
                return {'campaigns':rows}
            d=request.get_json(silent=True) or {}
            subject=str(d.get('subject','')).strip();body=str(d.get('body','')).strip()
            if not 3<=len(subject)<=120 or not 20<=len(body)<=10000:
                raise CommerceError('Use a subject of 3–120 characters and a message of 20–10,000 characters.')
            if '\r' in subject or '\n' in subject:raise CommerceError('Subject must be one line.')
            result=self.db.table('corner_campaigns').insert({'subject':subject,'body':body,'environment':self.env}).execute()
            return {'campaign':result.data[0]}

        @route('/admin/campaigns/<cid>/send',['POST'])
        def campaign_send(cid):
            uid=self.uid()
            if not self.developer(uid):raise CommerceError('Forbidden.',403)
            if (request.get_json(silent=True) or {}).get('confirm')!='SEND':raise CommerceError('Confirm SEND after reviewing the draft.')
            if not self.marketing_enabled or not os.getenv('CORNER_POSTAL_ADDRESS') or not self.mail_enabled:
                raise CommerceError('Marketing is disabled until sender, address, consent, and delivery are configured.',503)
            with self.lock('campaign-publish'):
                c=self.one('corner_campaigns',id=cid,environment=self.env)
                if not c or c['state']!='draft':raise CommerceError('This draft is already queued or does not exist.',409)
                recent=self.db.table('corner_campaigns').select('id',count='exact').eq('environment',self.env).gte('sent_at',iso(now()-timedelta(days=30))).execute()
                if (recent.count or 0)>=2:raise CommerceError('The two-campaign limit for the last 30 days has been reached.',409)
                self.update('corner_campaigns',{'state':'sending','sent_at':iso()},id=cid)
                self.audit(uid,'campaign_publish:'+cid)
            return {'message':'Campaign queued for opted-in adult accounts. The scheduler sends it within the daily limit.'}

        @route('/delete-account',['POST'])
        def delete_account():
            uid=self.uid();a=self.account(uid)
            if (request.get_json(silent=True) or {}).get('confirmation')!='DELETE':raise CommerceError('Type DELETE to confirm.')
            with self.lock('billing:'+self.env+':'+uid):
                if self.rows('gyms',owner_id=uid):raise CommerceError('Delete your owned gym in Gyms before deleting your account.',409)
                sessions=self.rows('class_sessions',user_id=uid)
                if any(s.get('status') in {'queued','pending','processing'} for s in sessions):
                    raise CommerceError('Wait for the current class generation to finish first.',409)
                self.update('corner_accounts',{'deletion_pending':True,'marketing':False,'training_emails':False},user_id=uid)
                # Refuse to delete if another environment has a subscription requiring cancellation.
                for b in self.rows('corner_billing',user_id=uid):
                    if b.get('subscription_id') and b['status'] not in TERMINAL:
                        if b['environment']!=self.env:raise CommerceError('Contact support to close billing in the other environment before account deletion.',409)
                        self.stripe('DELETE','subscriptions/'+b['subscription_id'],{'prorate':'false','invoice_now':'false'},'delete-account:'+b['subscription_id'])
                        self.reconcile(uid)
                self.audit(uid,'account_deletion_requested')
                paths=[s['storage_path'] for s in sessions if s.get('storage_path')]
                # Storage cleanup before DB deletion preserves the list on retries.
                for i in range(0,len(paths),100):self.db.storage.from_(os.getenv('SUPABASE_BUCKET','audio')).remove(paths[i:i+100])
                self.db.rpc('corner_delete_account_data',{'p_uid':uid}).execute()
                # Financial records retained, provider marketing contacts not used by this integration.
                self.db.auth.admin.delete_user(uid)
            return {'message':'Your account was deleted and subscription renewal stopped. Necessary billing/legal records may be retained.'}

        @route('/legal/<kind>')
        def legal(kind):
            from corner_policies import policy
            title,body=policy(kind,self.site,self.support)
            return self.public_page(title,body)


def install_commerce(app, db, verify, legacy_plan, developer):
    return Commerce(app,db,verify,legacy_plan,developer)
