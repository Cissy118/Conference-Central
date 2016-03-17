#!/usr/bin/env python

"""
conference.py -- Udacity conference server-side Python App Engine API;
    uses Google Cloud Endpoints

$Id: conference.py,v 1.25 2014/05/24 23:42:19 wesc Exp wesc $

created by wesc on 2014 apr 21

"""

from datetime import datetime
import json
import os
import time

import endpoints
from protorpc import messages
from protorpc import message_types
from protorpc import remote

from google.appengine.api import urlfetch
from google.appengine.ext import ndb
from google.appengine.api import memcache
from google.appengine.api import taskqueue

from models import StringMessage
from models import Profile
from models import ProfileMiniForm
from models import ProfileForm
from models import TeeShirtSize
from models import Conference
from models import ConferenceForm
from models import ConferenceForms
from models import ConferenceQueryForm
from models import ConferenceQueryForms
from models import Session
from models import SessionForm
from models import ConferenceSessionForms
from models import BooleanMessage
from models import ConflictException

from settings import WEB_CLIENT_ID

from utils import getUserId

EMAIL_SCOPE = endpoints.EMAIL_SCOPE
API_EXPLORER_CLIENT_ID = endpoints.API_EXPLORER_CLIENT_ID
MEMCACHE_ANNOUNCEMENTS_KEY = "RECENT_ANNOUNCEMENTS"
MEMCACHE_FEATURED_KEY = "RECENT_FT"

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
DEFAULTS = {
    "city": "Default City",
    "maxAttendees": 0,
    "seatsAvailable": 0,
    "topics": ["Default", "Topic"],
}
OPERATORS = {
        'EQ':   '=',
        'GT':   '>',
        'GTEQ': '>=',
        'LT':   '<',
        'LTEQ': '<=',
        'NE':   '!='
        }

FIELDS = {
            'CITY': 'city',
            'TOPIC': 'topics',
            'MONTH': 'month',
            'MAX_ATTENDEES': 'maxAttendees',
            }

CONF_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    websafeConferenceKey=messages.StringField(1),
)

CONF_POST_REQUEST = endpoints.ResourceContainer(
    ConferenceForm,
    websafeConferenceKey=messages.StringField(1),
)

SESS_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    websafeConferenceKey=messages.StringField(1),
)
SESS_POST_REQUEST = endpoints.ResourceContainer(
    SessionForm,
    websafeConferenceKey=messages.StringField(1),
)
SESSTYPE_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    websafeConferenceKey=messages.StringField(1),
    sessionType=messages.StringField(2),
)
SESS_SPEAKER_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    speaker=messages.StringField(1),
)
SESS_WISH_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    websafeSessionKey=messages.StringField(1),
)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


@endpoints.api(name='conference', version='v1',
               allowed_client_ids=[WEB_CLIENT_ID, API_EXPLORER_CLIENT_ID],
               scopes=[EMAIL_SCOPE])
class ConferenceApi(remote.Service):
    """Conference API v0.1"""

# - - - Profile objects - - - - - - - - - - - - - - - - - - -

    def _copyProfileToForm(self, prof):
        """Copy relevant fields from Profile to ProfileForm."""
        # copy relevant fields from Profile to ProfileForm
        pf = ProfileForm()
        for field in pf.all_fields():
            if hasattr(prof, field.name):
                # convert t-shirt string to Enum; just copy others
                if field.name == 'teeShirtSize':
                    setattr(pf, field.name, getattr(TeeShirtSize,
                            getattr(prof, field.name)))
                else:
                    setattr(pf, field.name, getattr(prof, field.name))
        pf.check_initialized()
        return pf

    def _getProfileFromUser(self):
        """Return user Profile from datastore,
        creating new one if non-existent."""
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        # get Profile from datastore
        user_id = getUserId(user)
        p_key = ndb.Key(Profile, user_id)
        profile = p_key.get()
        # create new Profile if not there
        if not profile:
            profile = Profile(
                key=p_key,
                displayName=user.nickname(),
                mainEmail=user.email(),
                teeShirtSize=str(TeeShirtSize.NOT_SPECIFIED),
            )
            profile.put()

        return profile      # return Profile

    def _doProfile(self, save_request=None):
        """Get user Profile and return to user, possibly updating it first."""
        # get user Profile
        prof = self._getProfileFromUser()
        # if saveProfile(), process user-modifyable fields
        if save_request:
            for field in ('displayName', 'teeShirtSize'):
                if hasattr(save_request, field):
                    val = getattr(save_request, field)
                    if val:
                        setattr(prof, field, str(val))
            prof.put()
        # return ProfileForm
        return self._copyProfileToForm(prof)

    @endpoints.method(message_types.VoidMessage, ProfileForm,
                      path='profile', http_method='GET', name='getProfile')
    def getProfile(self, request):
        """Return user profile."""
        return self._doProfile()

    @endpoints.method(ProfileMiniForm, ProfileForm,
                      path='profile', http_method='POST', name='saveProfile')
    def saveProfile(self, request):
        """Update & return user profile."""
        return self._doProfile(request)
# - - - Conference objects - - - - - - - - - - - - - - - - -

    def _copyConferenceToForm(self, conf, displayName):
        """Copy relevant fields from Conference to ConferenceForm."""
        cf = ConferenceForm()
        for field in cf.all_fields():
            if hasattr(conf, field.name):
                # convert Date to date string; just copy others
                if field.name.endswith('Date'):
                    setattr(cf, field.name, str(getattr(conf, field.name)))
                else:
                    setattr(cf, field.name, getattr(conf, field.name))
            elif field.name == "websafeKey":
                setattr(cf, field.name, conf.key.urlsafe())
        if displayName:
            setattr(cf, 'organizerDisplayName', displayName)
        cf.check_initialized()
        return cf

    def _createConferenceObject(self, request):
        """Create Conference object, returning ConferenceForm/request."""
        # preload necessary data items
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        user_id = getUserId(user)

        if not request.name:
            raise endpoints.BadRequestException(
                "Conference 'name' field required")

        # copy ConferenceForm/ProtoRPC Message into dict
        data = {field.name: getattr(request, field.name)
                for field in request.all_fields()}
        del data['websafeKey']
        del data['organizerDisplayName']

        # add default values for those missing
        # (both data model & outbound Message)
        for df in DEFAULTS:
            if data[df] in (None, []):
                data[df] = DEFAULTS[df]
                setattr(request, df, DEFAULTS[df])

        # convert dates from strings to Date objects;
        # set month based on start_date
        if data['startDate']:
            data['startDate'] = datetime.strptime(
                data['startDate'][:10], "%Y-%m-%d").date()
            data['month'] = data['startDate'].month
        else:
            data['month'] = 0
        if data['endDate']:
            data['endDate'] = datetime.strptime(
                data['endDate'][:10], "%Y-%m-%d").date()

        # set seatsAvailable to be same as maxAttendees on creation
        # both for data model & outbound Message
        if data["maxAttendees"] > 0:
            data["seatsAvailable"] = data["maxAttendees"]
            setattr(request, "seatsAvailable", data["maxAttendees"])

        # make Profile Key from user ID
        p_key = ndb.Key(Profile, user_id)
        # allocate new Conference ID with Profile key as parent
        c_id = Conference.allocate_ids(size=1, parent=p_key)[0]
        # make Conference key from ID
        c_key = ndb.Key(Conference, c_id, parent=p_key)
        data['key'] = c_key
        data['organizerUserId'] = request.organizerUserId = user_id

        # create Conference & return (modified) ConferenceForm
        Conference(**data).put()
        taskqueue.add(params={'email': user.email(),
                      'conferenceInfo': repr(request)},
                      url='/tasks/send_confirmation_email')
        return request

    @endpoints.method(ConferenceForm, ConferenceForm, path='conference',
                      http_method='POST', name='createConference')
    def createConference(self, request):
        """Create new conference."""
        return self._createConferenceObject(request)

    @endpoints.method(CONF_GET_REQUEST, ConferenceForm,
                      path='conference/{websafeConferenceKey}',
                      http_method='GET', name='getConference')
    def getConference(self, request):
        """Return requested conference (by websafeConferenceKey)."""
        # get Conference object from request; bail if not found
        conf = ndb.Key(urlsafe=request.websafeConferenceKey).get()
        if not conf:
            raise endpoints.NotFoundException(
                'No conference found with key: %s'
                % request.websafeConferenceKey)
        prof = conf.key.parent().get()
        # return ConferenceForm
        return self._copyConferenceToForm(conf, getattr(prof, 'displayName'))

    @endpoints.method(ConferenceQueryForms, ConferenceForms,
                      path='queryConferences',
                      http_method='POST',
                      name='queryConferences')
    def queryConferences(self, request):
        """Query for conferences."""
        conferences = self._getQuery(request)
        # return individual ConferenceForm object per Conference
        return ConferenceForms(
            items=[self._copyConferenceToForm(conf, "")
                   for conf in conferences])

    @endpoints.method(message_types.VoidMessage, ConferenceForms,
                      path='getConferencesCreated',
                      http_method='POST', name='getConferencesCreated')
    def getConferencesCreated(self, request):
        """Return conferences created by user."""
        # make sure user is authed
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')

        # make profile key
        p_key = ndb.Key(Profile, getUserId(user))
        # create ancestor query for this user
        conferences = Conference.query(ancestor=p_key)
        # get the user profile and display name
        prof = p_key.get()
        displayName = getattr(prof, 'displayName')
        # return set of ConferenceForm objects per Conference
        return ConferenceForms(
            items=[self._copyConferenceToForm(conf, displayName)
                   for conf in conferences]
        )

    def _getQuery(self, request):
        """Return formatted query from the submitted filters."""
        q = Conference.query()
        inequality_filter, filters = self._formatFilters(request.filters)

        # If exists, sort on inequality filter first
        if not inequality_filter:
            q = q.order(Conference.name)
        else:
            q = q.order(ndb.GenericProperty(inequality_filter))
            q = q.order(Conference.name)

        for filtr in filters:
            if filtr["field"] in ["month", "maxAttendees"]:
                filtr["value"] = int(filtr["value"])
            formatted_query = ndb.query.FilterNode(
                filtr["field"], filtr["operator"], filtr["value"])
            q = q.filter(formatted_query)
        return q

    def _formatFilters(self, filters):
        """Parse, check validity and format user supplied filters."""
        formatted_filters = []
        inequality_field = None

        for f in filters:
            filtr = {
                field.name: getattr(f, field.name) for field in f.all_fields()}

            try:
                filtr["field"] = FIELDS[filtr["field"]]
                filtr["operator"] = OPERATORS[filtr["operator"]]
            except KeyError:
                raise endpoints.BadRequestException(
                    "Filter contains invalid field or operator.")

            # Every operation except "=" is an inequality
            if filtr["operator"] != "=":
                if inequality_field and inequality_field != filtr["field"]:
                    raise endpoints.BadRequestException(
                        "Inequality filter is allowed on only one field.")
                else:
                    inequality_field = filtr["field"]

            formatted_filters.append(filtr)
        return (inequality_field, formatted_filters)

# - - - Session objects - - - - - - - - - - - - - - - - - -

    @endpoints.method(SESS_GET_REQUEST, ConferenceSessionForms,
                      path='conference/{websafeConferenceKey}/sessions',
                      http_method='GET', name='getConferenceSessions')
    def getConferenceSessions(self, request):
        """Given a conference, return all sessions"""

        # Get conference key
        wsck = request.websafeConferenceKey
        c_key = ndb.Key(urlsafe=wsck).get().key
        # Create ancestor query for this conference
        sessions = Session.query(ancestor=c_key)

        return ConferenceSessionForms(
            items=[self._copySessionToForm(session) for session in sessions]
        )

    @endpoints.method(
        SESSTYPE_GET_REQUEST, ConferenceSessionForms,
        path='conference/{websafeConferenceKey}/sessionsByType',
        http_method='GET', name='getConferenceSessionsByType')
    def getConferenceSessionByType(self, request):
        """Given a conference, return all the sessions of a specified type"""
        # Make conference key
        wsck = request.websafeConferenceKey
        s_type = request.sessionType
        c_key = ndb.Key(urlsafe=wsck).get().key
        # Create ancestor query for this conference
        sessions = Session.query(ancestor=c_key)
        sessionsByType = sessions.filter(Session.typeOfSession == s_type)
        return ConferenceSessionForms(
            items=[
                self._copySessionToForm(session) for session in sessionsByType]
        )

    @endpoints.method(
        SESS_SPEAKER_GET_REQUEST, ConferenceSessionForms,
        path='sessionsBySpeaker',
        http_method='GET', name='getSessionsBySpeaker')
    def getSessionBySpeaker(self, request):
        """Return all sessions by the specified speaker"""
        # Given a speaker, return all sessions given by this particular speaker
        # across all conferences
        spkr = request.speaker
        sessionsBySpeaker = Session.query().filter(Session.speaker == spkr)
        return ConferenceSessionForms(
            items=[self._copySessionToForm(session)
                   for session in sessionsBySpeaker])

    @endpoints.method(SESS_POST_REQUEST, SessionForm,
                      path='conference/{websafeConferenceKey}/session',
                      http_method='POST', name='createSession')
    def createSession(self, request):
        """Create new session."""
        # open only to the organizer of the conference
        return self._createSessionObject(request)

    @endpoints.method(
        message_types.VoidMessage, ConferenceSessionForms,
        path='sessionsSpecific',
        http_method='GET', name='getSpecificSessions')
    def getSpecificSessions(self, request):
        """Get all non-Workshop sessions before 19:00"""
        # Query all the sessions before 19:00 use an inequality filter
        timeMark = datetime.strptime("19:00", "%H:%M").time()
        sessions = Session.query().filter(Session.startTime < timeMark)
        # Use a for loop to filter all the non-workshop sessions,
        # to avoid one inequality filter restriction.
        # I know I use this way would avoid composite indexes either,
        # but I didn't find any other way to do this.
        final_sessions = []
        for session in sessions:
            if session.typeOfSession != "Workshop":
                final_sessions.append(session)
        return ConferenceSessionForms(
            items=[self._copySessionToForm(session)
                   for session in final_sessions])

    def _copySessionToForm(self, sess):
        """Copy relevant fields from Session to SessionForm."""
        sf = SessionForm()
        for field in sf.all_fields():
            if hasattr(sess, field.name):
                # Convert Time to time string; just copy others
                if field.name.endswith('Time'):
                    setattr(sf, field.name, str(getattr(sess, field.name)))
                else:
                    setattr(sf, field.name, getattr(sess, field.name))
            elif field.name == "websafeKey":
                setattr(sf, field.name, sess.key.urlsafe())
        sf.check_initialized()
        return sf

    def _createSessionObject(self, request):
        """Create or Update Session Object, returning SessionForm/request."""
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        user_id = getUserId(user)
        # check if conf exists given websafeConfKey
        # get conference; check that it exists
        wsck = request.websafeConferenceKey
        conf = ndb.Key(urlsafe=wsck).get()
        if not conf:
            raise endpoints.NotFoundException(
                'No conference found with key: %s' % wsck)

        # check if the user is organizer
        if conf.organizerUserId != user_id:
            raise endpoints.BadRequestException(
                "You should be the organizer to create sessions.")

        # copy SessionForm/ProtoRPC Message into dict
        data = {field.name: getattr(request, field.name)
                for field in request.all_fields()}
        del data['websafeKey']
        del data['websafeConferenceKey']

        # convert times from string to Time objects;
        if data['startTime']:
            data['startTime'] = datetime.strptime(
                data['startTime'][:5], "%H:%M").time()

        # ID based on Conference key get Session Key from ID
        s_id = Session.allocate_ids(size=1, parent=conf.key)[0]
        s_key = ndb.Key(Session, s_id, parent=conf.key)
        data['key'] = s_key

        # Create Session
        Session(**data).put()
        # Add Featured Speaker task
        speaker = data['speaker']
        taskqueue.add(
            params={'websafeConferenceKey': request.websafeConferenceKey,
                    'speaker': data['speaker']},
            url='/tasks/set_featured_speaker',
            method='GET')
        # Get session object for return result
        session = s_key.get()
        return self._copySessionToForm(session)

# - - - Wishlist - - - - - - - - - - - - - - - - - - - - - -

    def _sessionWishlist(self, request, reg=True):
        """add or delete session in wishlist"""
        retval = None
        # get profile from user
        prof = self._getProfileFromUser()
        # check if session exists given sessionKey
        sk = request.websafeSessionKey
        session = ndb.Key(urlsafe=sk).get()
        if not session:
            raise endpoints.NotFoundException(
                'No session found with key: %s' % sk)
        # Add
        if reg:
            # Check if user already added this session otherwise add
            if sk in prof.sessionWishlist:
                raise ConflictException(
                    "You have already added this session.")
            # Add user
            prof.sessionWishlist.append(sk)
            retval = True
        # delete
        else:
            # Check if user already added this session
            if sk in prof.sessionWishlist:
                # delete
                prof.sessionWishlist.remove(sk)
                retval = True
            else:
                retval = False
        # write things back to the datastore & return
        prof.put()
        return BooleanMessage(data=retval)

    @endpoints.method(
        message_types.VoidMessage, ConferenceSessionForms,
        path='session',
        http_method='GET', name='getSessionInWishlist')
    def getSessionInWishlist(self, request):
        """get list of sessions that user has in wishlist"""
        prof = self._getProfileFromUser()
        session_keys = [ndb.Key(urlsafe=sk) for sk in prof.sessionWishlist]
        sessions = ndb.get_multi(session_keys)
        return ConferenceSessionForms(
            items=[self._copySessionToForm(s) for s in sessions]
        )

    @endpoints.method(
        SESS_WISH_GET_REQUEST, BooleanMessage,
        path='session/{websafeSessionKey}',
        http_method='POST', name='addSessionToWishlist')
    def addSessionToWishlist(self, request):
        """Add session to wishlist"""
        return self._sessionWishlist(request)

    @endpoints.method(
        SESS_WISH_GET_REQUEST, BooleanMessage,
        path='session/{websafeSessionKey}',
        http_method='DELETE', name='deleteSessionInWishlist')
    def deleteSessionInWishlist(self, request):
        """Delete session from wishlist"""
        return self._sessionWishlist(request, reg=False)

# - - - Registration - - - - - - - - - - - - - - - - - - - -

    @ndb.transactional(xg=True)
    def _conferenceRegistration(self, request, reg=True):
        """Register or unregister user for selected conference."""
        retval = None
        prof = self._getProfileFromUser()  # get user Profile

        # check if conf exists given websafeConfKey
        # get conference; check that it exists
        wsck = request.websafeConferenceKey
        conf = ndb.Key(urlsafe=wsck).get()
        if not conf:
            raise endpoints.NotFoundException(
                'No conference found with key: %s' % wsck)

        # register
        if reg:
            # check if user already registered otherwise add
            if wsck in prof.conferenceKeysToAttend:
                raise ConflictException(
                    "You have already registered for this conference")

            # check if seats avail
            if conf.seatsAvailable <= 0:
                raise ConflictException(
                    "There are no seats available.")

            # register user, take away one seat
            prof.conferenceKeysToAttend.append(wsck)
            conf.seatsAvailable -= 1
            retval = True

        # unregister
        else:
            # check if user already registered
            if wsck in prof.conferenceKeysToAttend:

                # unregister user, add back one seat
                prof.conferenceKeysToAttend.remove(wsck)
                conf.seatsAvailable += 1
                retval = True
            else:
                retval = False

        # write things back to the datastore & return
        prof.put()
        conf.put()
        return BooleanMessage(data=retval)

    @endpoints.method(
        CONF_GET_REQUEST, BooleanMessage,
        path='conference/{websafeConferenceKey}',
        http_method='POST', name='registerForConference')
    def registerForConference(self, request):
        """Register user for selected conference."""
        return self._conferenceRegistration(request)

    @endpoints.method(
        CONF_GET_REQUEST, BooleanMessage,
        path='conference/{websafeConferenceKey}',
        http_method='DELETE', name='unregisterFromConference')
    def unregisterFromConference(self, request):
        """Unregister user for selected conference."""
        return self._conferenceRegistration(request, reg=False)

    @endpoints.method(
        message_types.VoidMessage, ConferenceForms,
        path='conferences/attending',
        http_method='GET', name='getConferencesToAttend')
    def getConferencesToAttend(self, request):
        """Get list of conferences that user has registered for."""
        prof = self._getProfileFromUser()
        conf_keys = [
            ndb.Key(urlsafe=wsck) for wsck in prof.conferenceKeysToAttend]
        conferences = ndb.get_multi(conf_keys)
        # return set of ConferenceForm objects per Conference
        return ConferenceForms(
            items=[
                self._copyConferenceToForm(conf, "") for conf in conferences])

# - - - Featured Speakers - - - - - - - - - - - - - - - - - -

    @staticmethod
    def _cacheFeaturedSpeaker(websafeConferenceKey, speaker):
        """Get featured speaker entry to memcache"""
        # sessions of this conference with this speaker
        print websafeConferenceKey
        c_key = ndb.Key(urlsafe=websafeConferenceKey).get().key
        sessions = Session.query(
            ancestor=c_key).filter(Session.speaker == speaker)
        count = len(sessions.fetch())
        c_name = c_key.get().name
        # check if more than one session by this speaker
        if count > 1:
            featured = '%s Featured Speakers: %s has sessions %s' % (
                c_name, speaker, ','.join(s.name for s in sessions))
            memcache.set(MEMCACHE_FEATURED_KEY, featured)
        else:
            featured = ""
            memcache.delete(MEMCACHE_FEATURED_KEY)
        return featured

    @endpoints.method(message_types.VoidMessage, StringMessage,
                      path='session/featured/get',
                      http_method='GET', name='getFeaturedSpeaker')
    def getFeaturedSpeaker(self, request):
        """get featured speaker"""
        return StringMessage(data=memcache.get(MEMCACHE_FEATURED_KEY) or "")

# - - - Announcements - - - - - - - - - - - - - - - - - - - -

    @staticmethod
    def _cacheAnnouncement():
        """Create Announcement & assign to memcache; used by
        memcache cron job & putAnnouncement().
        """
        confs = Conference.query(ndb.AND(
            Conference.seatsAvailable <= 5,
            Conference.seatsAvailable > 0)
        ).fetch(projection=[Conference.name])

        if confs:
            # If there are almost sold out conferences,
            # format announcement and set it in memcache
            announcement = '%s %s' % (
                'Last chance to attend! The following conferences '
                'are nearly sold out:',
                ', '.join(conf.name for conf in confs))
            memcache.set(MEMCACHE_ANNOUNCEMENTS_KEY, announcement)
        else:
            # If there are no sold out conferences,
            # delete the memcache announcements entry
            announcement = ""
            memcache.delete(MEMCACHE_ANNOUNCEMENTS_KEY)
        return announcement

    @endpoints.method(message_types.VoidMessage, StringMessage,
                      path='conference/announcement/get',
                      http_method='GET', name='getAnnouncement')
    def getAnnouncement(self, request):
        """Return Announcement from memcache."""
        announcement = memcache.get(MEMCACHE_ANNOUNCEMENTS_KEY)
        if not announcement:
            announcement = ""
        return StringMessage(data=announcement)


# registers API
api = endpoints.api_server([ConferenceApi])
