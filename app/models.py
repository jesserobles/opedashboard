from datetime import datetime
from dateutil import tz
import os
import csv
import hashlib

from werkzeug.security import generate_password_hash, check_password_hash
from flask import current_app, request, url_for
from flask_login import UserMixin, AnonymousUserMixin
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from markdown import markdown
import bleach
from . import db
from . import login_manager


class Permission:
    READ = 0x01
    WRITE = 0x02
    APPROVE = 0x04
    ADMINISTER = 0x80


class Role(db.Model):
    __tablename__ = 'roles'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), unique=True)
    default = db.Column(db.Boolean, default=False, index=True)
    permissions = db.Column(db.Integer)
    users = db.relationship('User', backref='role', lazy='dynamic')

    @staticmethod
    def insert_roles():
        roles = {
            'User': (Permission.READ, True),
            'Author': (Permission.READ |
                       Permission.WRITE, False),
            'Approver': (Permission.READ |
                         Permission.WRITE |
                         Permission.APPROVE, False),
            'Administrator': (0xff, False)
        }
        for r in roles:
            role = Role.query.filter_by(name=r).first()
            if role is None:
                role = Role(name=r)
            role.permissions = roles[r][0]
            role.default = roles[r][1]
            db.session.add(role)
        db.session.commit()

    def __repr__(self):
        return '<Role %r>' % self.name


class User(UserMixin, db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(64), unique=True, index=True)
    username = db.Column(db.String(64), unique=True, index=True)
    password_hash = db.Column(db.String(128))
    role_id = db.Column(db.Integer, db.ForeignKey('roles.id'))
    confirmed = db.Column(db.Boolean, default=False)
    first_name = db.Column(db.String(64))
    last_name = db.Column(db.String(64))
    location = db.Column(db.String(64))
    # phone = db.Column(db.Integer)
    about_me = db.Column(db.Text())
    member_since = db.Column(db.DateTime(), default=datetime.utcnow)
    last_seen = db.Column(db.DateTime(), default=datetime.utcnow)
    avatar_hash = db.Column(db.String(32))

    def __init__(self, **kwargs):
        super(User, self).__init__(**kwargs)
        if self.role is None:
            if self.email == current_app.config['OPEDATABASE_ADMIN']:
                self.confirmed = True
                self.role = Role.query.filter_by(permissions=0xff).first()
            if self.role is None:
                self.role = Role.query.filter_by(default=True).first()
            if self.email is not None and self.avatar_hash is None:
                self.avatar_hash = hashlib.md5(self.email.encode('utf-8')).hexdigest()

    @property
    def password(self):
        raise AttributeError('password is not a readable attribute')

    @password.setter
    def password(self, password):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        return check_password_hash(self.password_hash, password)

    def generate_confirmation_token(self, expiration=60*60*48):
        s = Serializer(current_app.config['SECRET_KEY'], expiration)
        return s.dumps({'confirm': self.id})

    def generate_auth_token(self, expiration):
        s = Serializer(current_app.config['SECRET_KEY'], expires_in=expiration)
        return s.dumps({'id': self.id}).decode('ascii')

    @staticmethod
    def verify_auth_token(token):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token)
        except:
            return None
        return User.query.get(data['id'])

    def confirm(self, token):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token)
        except:
            return False
        if data.get('confirm') != self.id:
            return False
        self.confirmed = True
        db.session.add(self)
        return True

    def generate_reset_token(self, expiration=60*60*48):
        s = Serializer(current_app.config['SECRET_KEY'], expiration)
        return s.dumps({'reset': self.id})

    def reset_password(self, token, new_password):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token)
        except:
            return False
        if data.get('reset') != self.id:
            return False
        self.password = new_password
        db.session.add(self)
        return True

    def generate_email_change_token(self, new_email, expiration=60*60*48):
        s = Serializer(current_app.config['SECRET_KEY'], expiration)
        return s.dumps({'change_email': self.id, 'new_email': new_email})

    def change_email(self, token):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token)
        except:
            return False
        if data.get('change_email') != self.id:
            return False
        new_email = data.get('new_email')
        if new_email is None:
            return False
        if self.query.filter_by(email=new_email).first() is not None:
            return False
        self.email = new_email
        self.avatar_hash = hashlib.md5(self.email.encode('utf-8')).hexdigest()
        db.session.add(self)
        return True

    def can(self, permissions):
        return self.role is not None and (self.role.permissions & permissions) == permissions

    def is_administrator(self):
        return self.can(Permission.ADMINISTER)

    def ping(self):
        self.last_seen = datetime.utcnow()
        db.session.add(self)

    def gravatar(self, size=100, default='identicon', rating='g'):
        if request.is_secure:
            url = 'https://secure.gravatar.com/avatar'
        else:
            url = 'http://www.gravatar.com/avatar'
        hash = self.avatar_hash or hashlib.md5(self.email.encode('utf-8')).hexdigest()
        return '{url}/{hash}?s={size}&d={default}&r={rating}'.format(url=url, hash=hash, size=size, default=default,
                                                                     rating=rating)

    @staticmethod
    def generate_fake(count=100):
        from db.exc import IntegrityError
        from random import seed
        import forgery_py

        seed()
        for i in range(count):
            u = User(email=forgery_py.internet.email_address(),
                     username=forgery_py.internet.user_name(True),
                     password=forgery_py.lorem_ipsum.word(),
                     confirmed=True,
                     first_name=forgery_py.name.first_name(),
                     last_name=forgery_py.name.last_name(),
                     location=forgery_py.address.city(),
                     about_me=forgery_py.lorem_ipsum.sentence(),
                     member_since=forgery_py.date.date(True))
            db.session.add(u)
            try:
                db.session.commit()
            except IntegrityError:
                db.session.rollback()

    def __repr__(self):
        return '<User %r>' % self.username


class AnonymousUser(AnonymousUserMixin):
    def can(self, permissions):
        return False

    def is_administrator(self):
        return False


login_manager.anonymous_user = AnonymousUser


class Document(db.Model):
    __tablename__ = 'adamsdocuments'
    id = db.Column(db.Integer, primary_key=True)
    accessionnumber = db.Column(db.String(50), nullable=False)
    addresseeaffiliation = db.Column(db.String(15000))
    addresseename = db.Column(db.String(3000))
    authoraffiliation = db.Column(db.String(3000))
    authorname = db.Column(db.String(2000))
    casereferencenumber = db.Column(db.String(3000))
    compounddocumentstate = db.Column(db.Boolean)
    contentsize = db.Column(db.Integer)
    datedocketed = db.Column(db.DateTime)
    docketnumber = db.Column(db.String(3000))
    documentdate = db.Column(db.Date)
    documentreportnumber = db.Column(db.String(1000))
    documenttitle = db.Column(db.String(1000))
    documenttype = db.Column(db.String(500))
    estimatedpagecount = db.Column(db.Integer)
    keyword = db.Column(db.String(3500))
    licensenumber = db.Column(db.String(1500))
    mimetype = db.Column(db.String(50))
    packagenumber = db.Column(db.String(300))
    publishdatepars = db.Column(db.DateTime)
    uri = db.Column(db.String(1000))
    
    
    def __repr__(self):
        return "<Document(accession='%s')>" % (self.accessionnumber)


    def to_json(self):
        json = {
            'id': self.id,
            'accessionnumber': self.accessionnumber,
            'addresseeaffiliation': self.addresseeaffiliation,
            'addresseename': self.addresseename,
            'authoraffiliation': self.authoraffiliation,
            'authorname': self.authorname,
            'casereferencenumber': self.casereferencenumber,
            'compounddocumentstate': self.compounddocumentstate,
            'contentsize': self.contentsize,
            'datedocketed': self.datedocketed,
            'docketnumber': self.docketnumber,
            'documentdate': self.documentdate,
            'documentreportnumber': self.documentreportnumber,
            'documenttitle': self.documenttitle,
            'documenttype': self.documenttype,
            'estimatedpagecount': self.estimatedpagecount,
            'keyword': self.keyword,
            'licensenumber': self.licensenumber,
            'mimetype': self.mimetype,
            'packagenumber': self.packagenumber,
            'publishdatepars': self.publishdatepars,
            'uri': self.uri
        }

        return json


class EventNotification(db.Model):
    __tablename__ = 'eventnotifications'
    id = db.Column(db.Integer, primary_key=True)
    eventdesc = db.Column(db.String(50))
    enno = db.Column(db.Integer, nullable=False, index=True)
    sitename = db.Column(db.String(50))
    licenseename = db.Column(db.String(100))
    regionno = db.Column(db.Integer)
    cityname = db.Column(db.String(50))
    statecd = db.Column(db.String(2))
    countyname = db.Column(db.String(50))
    licenseno = db.Column(db.String(50))
    agreementstateind = db.Column(db.Boolean)
    docketno = db.Column(db.String(50))
    unitind1 = db.Column(db.Integer)
    unitind2 = db.Column(db.Integer)
    unitind3 = db.Column(db.Integer)
    reactortype = db.Column(db.String(50))
    nrcnotifiedby = db.Column(db.String(50))
    opsofficer = db.Column(db.String(50))
    notificationdt = db.Column(db.Date)
    notificationtime = db.Column(db.Time)
    eventdt = db.Column(db.Date)
    eventtime = db.Column(db.Time)
    timezone = db.Column(db.String(3))
    lastupdateddt = db.Column(db.Date)
    emergencyclass = db.Column(db.String(50))
    cfrcd1 = db.Column(db.String(50))
    cfrdescr1 = db.Column(db.String(50))
    cfrcd2 = db.Column(db.String(50))
    cfrdescr2 = db.Column(db.String(50))
    cfrcd3 = db.Column(db.String(50))
    cfrdescr3 = db.Column(db.String(50))
    cfrcd4 = db.Column(db.String(50))
    cfrdescr4 = db.Column(db.String(50))
    staffname1 = db.Column(db.String(50))
    orgabbrev1 = db.Column(db.String(10))
    staffname2 = db.Column(db.String(50))
    orgabbrev2 = db.Column(db.String(10))
    staffname3 = db.Column(db.String(50))
    orgabbrev3 = db.Column(db.String(10))
    staffname4 = db.Column(db.String(50))
    orgabbrev4 = db.Column(db.String(10))
    staffname5 = db.Column(db.String(50))
    orgabbrev5 = db.Column(db.String(10))
    staffname6 = db.Column(db.String(50))
    orgabbrev6 = db.Column(db.String(10))
    staffname7 = db.Column(db.String(50))
    orgabbrev7 = db.Column(db.String(10))
    staffname8 = db.Column(db.String(50))
    orgabbrev8 = db.Column(db.String(10))
    staffname9 = db.Column(db.String(50))
    orgabbrev9 = db.Column(db.String(10))
    staffname10 = db.Column(db.String(50))
    orgabbrev10 = db.Column(db.String(10))
    scramcode1 = db.Column(db.String(3))
    rxcrit1 = db.Column(db.Boolean)
    initialpwr1 = db.Column(db.Integer)
    initialrxmode1  = db.Column(db.String(25))
    currentpwr1 = db.Column(db.Integer)
    currentrxmode1  = db.Column(db.String(25))
    scramcode2 = db.Column(db.String(3))
    rxcrit2 = db.Column(db.Boolean)
    initialpwr2 = db.Column(db.Integer)
    initialrxmode2  = db.Column(db.String(25))
    currentpwr2 = db.Column(db.Integer)
    currentrxmode2  = db.Column(db.String(25))
    scramcode3 = db.Column(db.String(3))
    rxcrit3 = db.Column(db.Boolean)
    initialpwr3 = db.Column(db.Integer)
    initialrxmode3  = db.Column(db.String(25))
    currentpwr3 = db.Column(db.Integer)
    currentrxmode3  = db.Column(db.String(25))
    eventtext = db.Column(db.Text)
    comments = db.Column(db.Text)
    materialcategory = db.Column(db.String(20))
    retraction = db.Column(db.Boolean)

    
    def __repr__(self):
        return f'<EN {self.enno}>'

class PowerStatus(db.Model):
    __tablename__ = 'powerstatus'
    id = db.Column(db.Integer, primary_key=True)
    reportdate = db.Column(db.Date)
    unit = db.Column(db.String(50))
    region = db.Column(db.Integer)
    power = db.Column(db.Float)
    down = db.Column(db.Date)
    reasonorcomment = db.Column(db.String(1000))
    changeinreport = db.Column(db.Boolean)
    numberofscrams = db.Column(db.Integer)
    updated = db.Column(db.Boolean)
    
    
    def __repr__(self):
        return '<PowerStatus {}, {}>'.format(self.unit, self.reportdate)


class CFR(db.Model):
    __tablename__ = 'cfrcodes'
    id = db.Column(db.Integer, primary_key=True)
    cfr = db.Column(db.String(64), index=True)

    def __repr__(self):
        return '<CFR %r>' % self.cfr

    @staticmethod
    def insert_cfrs():
        with open('data/cfrs', 'r') as file:
            reader = csv.reader(file)
            codes = [line[0].strip() for line in reader]
        for c in codes:
            cfr = CFR.query.filter_by(cfr=c).first()
            if cfr is None:
                cfr = CFR(cfr=c)
            db.session.add(cfr)
        db.session.commit()
