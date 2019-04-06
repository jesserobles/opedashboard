from flask import render_template, session, redirect, url_for, abort,\
     flash, request, current_app, Response
from flask_login import login_required, current_user
import io
from . import main
from .forms import EditProfileForm, EditProfileAdminForm
from .. import db
from ..models import User, Role, Permission, Document, EventNotification, \
    PowerStatus, CFR
from ..decorators import admin_required, permission_required


@main.route('/', methods=['GET', 'POST'])
def index():
    return 'Hello, world!'