import decimal
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.hashers import make_password
from django.contrib import messages
from django.db.models import Count, Q
from django.http import (
    HttpResponseRedirect, HttpResponse)
from django.shortcuts import render_to_response, get_object_or_404
from django.template import RequestContext, loader
from django.contrib.auth.models import User
from datetime import date, timedelta, datetime
# from django.utils import simplejson
import json
import logging
import sys
import operator
import pandas
from django.shortcuts import render
import numpy
import time

from django.core.files.storage import FileSystemStorage
from django.core.urlresolvers import reverse
from django.db import (IntegrityError, transaction)
from django.db.models import ProtectedError
from django.shortcuts import redirect
from onadata.apps.main.models.user_profile import UserProfile
from onadata.apps.usermodule.forms import UserForm, UserProfileForm, ChangePasswordForm, UserEditForm, OrganizationForm, \
    OrganizationDataAccessForm, ResetPasswordForm
from onadata.apps.usermodule.models import UserModuleProfile, UserPasswordHistory, UserFailedLogin, Organizations, \
    OrganizationDataAccess
from django.contrib.auth.decorators import login_required, user_passes_test
from django import forms
# Menu imports
from onadata.apps.usermodule.forms import MenuForm
from onadata.apps.usermodule.models import MenuItem
# Unicef Imports
from onadata.apps.logger.models import Instance, XForm
# Organization Roles Import
from onadata.apps.usermodule.models import OrganizationRole, MenuRoleMap, UserRoleMap
from onadata.apps.usermodule.forms import OrganizationRoleForm, RoleMenuMapForm, UserRoleMapForm, UserRoleMapfForm
from django.forms.models import inlineformset_factory, modelformset_factory
from django.forms.formsets import formset_factory
from django.core.exceptions import ObjectDoesNotExist
from django.views.decorators.csrf import csrf_exempt
import os
from django.db import connection
from collections import OrderedDict
import math


def __db_fetch_values(query):
    cursor = connection.cursor()
    cursor.execute(query)
    fetchVal = cursor.fetchall()
    cursor.close()
    return fetchVal


def __db_fetch_single_value(query):
    cursor = connection.cursor()
    cursor.execute(query)
    fetchVal = cursor.fetchone()
    cursor.close()
    return fetchVal[0]


def __db_fetch_values_dict(query):
    cursor = connection.cursor()
    cursor.execute(query)
    fetchVal = dictfetchall(cursor)
    cursor.close()
    return fetchVal


def __db_commit_query(query):
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()


def dictfetchall(cursor):
    desc = cursor.description
    return [
        OrderedDict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()]


def decimal_date_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    elif hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        return obj
    raise TypeError


def householdprofile(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for division data
    # returns name,sampled,completed,percent completed
    # data stored in division_data
    query = "select t2.div_id,t2.division,case when sampled is null then 0 else sampled end as sampled,submission_goal, case when round((sampled/submission_goal)*100,2) is null then 0 else round((sampled/submission_goal)*100,2) end   as percent from  (select div_id,division ,sum(sampled) as sampled from (select user_id,count(*) as sampled from logger_instance where xform_id = " + str(
        form_id) + " and deleted_at is null group by user_id) as hhp ,  vw_geo_data_catchment  as sd where hhp.user_id = sd.user_id group by division,div_id) as t1 right outer join (select div_id,division,sum(submission_goal) as submission_goal from hhp_sampled hhp,  vw_geo_data_catchment as gc where hhp.user_id = gc.user_id group by division,div_id) as t2 on t1.div_id = t2.div_id"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)

    # for setting division_id and division_name
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # calculation
    total_sampled = 0
    for each in df.sampled.tolist():
        total_sampled = total_sampled + each
    total_goal = 0
    for each in df.submission_goal.tolist():
        total_goal = total_goal + each
    if total_goal != 0:
        total_percentage = (total_sampled / total_goal) * 100
    else:
        total_percentage = 0
    # for intial chart data load
    # query = "SELECT t1.date_created, SUM(CASE WHEN sampled IS NULL THEN 0 ELSE sampled END)AS sampled FROM(SELECT hhp.user_id, hhp.date_created AS date_created, div_id, division, SUM(sampled) AS sampled FROM (SELECT user_id, date_created :: DATE AS date_created, Count(*) AS sampled FROM logger_instance WHERE xform_id = 403 AND deleted_at IS NULL GROUP BY user_id, date_created :: DATE) AS hhp, vw_geo_data_catchment AS sd WHERE hhp.user_id = sd.user_id GROUP BY division, div_id, hhp.user_id, hhp.date_created) AS t1 right outer join (SELECT div_id, division, SUM(submission_goal) AS submission_goal FROM hhp_sampled hhp, vw_geo_data_catchment AS gc WHERE hhp.user_id = gc.user_id GROUP BY division, div_id) AS t2 ON t1.div_id = t2.div_id WHERE t1.date_created IS NOT NULL GROUP BY t1.date_created order by t1.date_created"
    query = "SELECT hhp.date_created AS date_created, SUM(sampled) AS sampled FROM(SELECT user_id, date_created :: DATE AS date_created, Count(*) AS sampled FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL GROUP BY user_id, date_created :: DATE) AS hhp, vw_geo_data_catchment AS sd WHERE hhp.user_id = sd.user_id GROUP BY hhp.date_created order by hhp.date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['sampled'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/householdprofile.html', {'division': division,
                                                                'divison_data': divison_data,
                                                                'total_sampled': total_sampled,
                                                                'total_goal': total_goal,
                                                                'total_percentage': total_percentage,
                                                                'categories': categories,
                                                                'data': data
                                                                })


def getDistricts(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns division wise data
        query = "select t2.div_id,t2.division,case when sampled is null then 0 else sampled end as sampled,submission_goal, case when round((sampled/submission_goal)*100,2) is null then 0 else round((sampled/submission_goal)*100,2) end   as percent from  (select div_id,division ,sum(sampled) as sampled from (select user_id,count(*) as sampled from logger_instance where xform_id = " + str(
            form_id) + " and deleted_at is null group by user_id) as hhp ,  vw_geo_data_catchment  as sd where hhp.user_id = sd.user_id group by division,div_id) as t1 right outer join (select div_id,division,sum(submission_goal) as submission_goal from hhp_sampled hhp,  vw_geo_data_catchment as gc where hhp.user_id = gc.user_id group by division,div_id) as t2 on t1.div_id = t2.div_id"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # calculation
        total_sampled = 0
        for each in df.sampled.tolist():
            total_sampled = total_sampled + each
        total_goal = 0
        for each in df.submission_goal.tolist():
            total_goal = total_goal + each
        if total_goal != 0:
            total_percentage = (total_sampled / total_goal) * 100
        else:
            total_percentage = 0
        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_sampled': total_sampled,
                                        'total_goal': total_goal,
                                        'total_percentage': total_percentage}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "SELECT t2.dist_id, t2.district, CASE WHEN sampled IS NULL THEN 0 ELSE sampled END AS sampled, submission_goal, CASE WHEN Round(( sampled / submission_goal) * 100, 2) IS NULL THEN 0 ELSE Round(( sampled / submission_goal ) * 100, 2) END AS PERCENT FROM(SELECT div_id,dist_id, district, Sum(sampled) AS sampled FROM (SELECT user_id, Count(*) AS sampled FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL GROUP BY user_id) AS hhp, vw_geo_data_catchment AS sd WHERE hhp.user_id = sd.user_id GROUP BY div_id,district, dist_id) AS t1 RIGHT OUTER JOIN (SELECT div_id,dist_id, district, Sum(submission_goal) AS submission_goal FROM hhp_sampled hhp, vw_geo_data_catchment AS gc WHERE hhp.user_id = gc.user_id GROUP BY div_id,district, dist_id) AS t2 ON t1.dist_id = t2.dist_id where t2.div_id =" + str(
        division)
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # calculation
    total_sampled = 0
    for each in df.sampled.tolist():
        total_sampled = total_sampled + each
    total_goal = 0
    for each in df.submission_goal.tolist():
        total_goal = total_goal + each
    if total_goal != 0:
        total_percentage = (total_sampled / total_goal) * 100
    else:
        total_percentage = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'district_data': district_data,
                                    'total_sampled': total_sampled,
                                    'total_goal': total_goal,
                                    'total_percentage': total_percentage}))


def getUsers(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "SELECT t2.dist_id, t2.district, CASE WHEN sampled IS NULL THEN 0 ELSE sampled END AS sampled, submission_goal, CASE WHEN Round(( sampled / submission_goal) * 100, 2) IS NULL THEN 0 ELSE Round(( sampled / submission_goal ) * 100, 2) END AS PERCENT FROM(SELECT div_id,dist_id, district, Sum(sampled) AS sampled FROM (SELECT user_id, Count(*) AS sampled FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL GROUP BY user_id) AS hhp, vw_geo_data_catchment AS sd WHERE hhp.user_id = sd.user_id GROUP BY div_id,district, dist_id) AS t1 RIGHT OUTER JOIN (SELECT div_id,dist_id, district, Sum(submission_goal) AS submission_goal FROM hhp_sampled hhp, vw_geo_data_catchment AS gc WHERE hhp.user_id = gc.user_id GROUP BY div_id,district, dist_id) AS t2 ON t1.dist_id = t2.dist_id where t2.div_id =" + str(
            request.POST.get('div'))
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # calculation
        total_sampled = 0
        for each in df.sampled.tolist():
            total_sampled = total_sampled + each
        total_goal = 0
        for each in df.submission_goal.tolist():
            total_goal = total_goal + each
        if total_goal != 0:
            total_percentage = (total_sampled / total_goal) * 100
        else:
            total_percentage = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_sampled': total_sampled,
                                        'total_goal': total_goal,
                                        'total_percentage': total_percentage}))
    else:
        # query returns user wise data
        query = "SELECT (select username from auth_user where id = t2.user_id)as username,t2.dist_id, t2.district, CASE WHEN sampled IS NULL THEN 0 ELSE sampled END AS sampled, submission_goal, CASE WHEN Round(( sampled / submission_goal) * 100, 2) IS NULL THEN 0 ELSE Round(( sampled / submission_goal ) * 100, 2)::float END AS PERCENT FROM (SELECT hhp.user_id,dist_id, district, Sum(sampled) AS sampled FROM (SELECT user_id, Count(*) AS sampled FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL GROUP BY user_id) AS hhp, vw_geo_data_catchment AS sd WHERE hhp.user_id = sd.user_id GROUP BY hhp.user_id,district, dist_id) AS t1 RIGHT OUTER JOIN (SELECT hhp.user_id,dist_id, district, Sum(submission_goal) AS submission_goal FROM hhp_sampled hhp, vw_geo_data_catchment AS gc WHERE hhp.user_id = gc.user_id GROUP BY hhp.user_id,district, dist_id) AS t2 ON t1.user_id = t2.user_id where t2.dist_id = " + str(
            district)
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # calculation
        total_sampled = 0
        for each in df.sampled.tolist():
            total_sampled = total_sampled + each
        total_goal = 0
        for each in df.submission_goal.tolist():
            total_goal = total_goal + each
        if total_goal != 0:
            total_percentage = (total_sampled / total_goal) * 100
        else:
            total_percentage = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_sampled': total_sampled,
                                        'total_goal': total_goal,
                                        'total_percentage': total_percentage}))


def getDistrictsCharts(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "SELECT hhp.date_created AS date_created, SUM(sampled) AS sampled FROM(SELECT user_id, date_created :: DATE AS date_created, Count(*) AS sampled FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL GROUP BY user_id, date_created :: DATE) AS hhp, vw_geo_data_catchment AS sd WHERE hhp.user_id = sd.user_id and div_id = " + str(
        division) + " GROUP BY div_id, hhp.date_created order by hhp.date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['sampled'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersCharts(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]
    # get district id
    district = request.POST.get('dist')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for division wise data
        query = "SELECT hhp.date_created AS date_created, SUM(sampled) AS sampled FROM(SELECT user_id, date_created :: DATE AS date_created, Count(*) AS sampled FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL GROUP BY user_id, date_created :: DATE) AS hhp, vw_geo_data_catchment AS sd WHERE hhp.user_id = sd.user_id and div_id = " + str(
            request.POST.get('div')) + " GROUP BY div_id, hhp.date_created order by hhp.date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['sampled'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "SELECT (select username from auth_user where id = t2.user_id)as username,t2.user_id,t2.dist_id, t2.district, CASE WHEN sampled IS NULL THEN 0 ELSE sampled END AS sampled, submission_goal, CASE WHEN Round(( sampled / submission_goal) * 100, 2) IS NULL THEN 0 ELSE Round(( sampled / submission_goal ) * 100, 2)::float END AS PERCENT FROM (SELECT hhp.user_id,dist_id, district, Sum(sampled) AS sampled FROM (SELECT user_id, Count(*) AS sampled FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL GROUP BY user_id) AS hhp, vw_geo_data_catchment AS sd WHERE hhp.user_id = sd.user_id GROUP BY hhp.user_id,district, dist_id) AS t1 RIGHT OUTER JOIN (SELECT hhp.user_id,dist_id, district, Sum(submission_goal) AS submission_goal FROM hhp_sampled hhp, vw_geo_data_catchment AS gc WHERE hhp.user_id = gc.user_id GROUP BY hhp.user_id,district, dist_id) AS t2 ON t1.user_id = t2.user_id where t2.dist_id = " + str(
            district)
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "SELECT hhp.date_created AS date_created, SUM(sampled) AS sampled FROM(SELECT user_id, date_created :: DATE AS date_created, Count(*) AS sampled FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL GROUP BY user_id, date_created :: DATE) AS hhp, vw_geo_data_catchment AS sd WHERE hhp.user_id = sd.user_id and dist_id = " + str(
            district) + " GROUP BY dist_id, hhp.date_created order by hhp.date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['sampled'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersData(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "SELECT hhp.date_created AS date_created, SUM(sampled) AS sampled FROM(SELECT user_id, date_created :: DATE AS date_created, Count(*) AS sampled FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL GROUP BY user_id, date_created :: DATE) AS hhp, vw_geo_data_catchment AS sd WHERE hhp.user_id = sd.user_id and dist_id = " + str(
            request.POST.get('dist')) + " GROUP BY dist_id, hhp.date_created order by hhp.date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['sampled'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "SELECT hhp.date_created AS date_created, SUM(sampled) AS sampled FROM(SELECT user_id, date_created :: DATE AS date_created, Count(*) AS sampled FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL GROUP BY user_id, date_created :: DATE) AS hhp, vw_geo_data_catchment AS sd WHERE hhp.user_id = sd.user_id and hhp.user_id = " + str(
            user_id) + " GROUP BY hhp.user_id, hhp.date_created order by hhp.date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['sampled'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))


def householdprofile2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_hh_form_submission,total_hh_member,ratio
    # data stored in division_data
    query = " WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id,division, Count(distinct household_id) total_hh_form_submission, SUM(hh_member) total_hh_member, case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2)  end AS ratio FROM f GROUP BY div_id,division"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)

    # for setting division_id and division_name
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # total ratio calculation
    total_hh_form_submission = 0
    for each in df.total_hh_form_submission.tolist():
        total_hh_form_submission = total_hh_form_submission + each
    total_hh_member = 0
    for each in df.total_hh_member.tolist():
        total_hh_member = total_hh_member + each
    if total_hh_member != 0:
        total_percentage = (float(total_hh_form_submission) / float(total_hh_member))
    else:
        total_ratio = 0

    # for intial chart data load
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/householdprofile2.html', {'division': division,
                                                                 'divison_data': divison_data,
                                                                 'total_hh_form_submission': total_hh_form_submission,
                                                                 'total_hh_member': total_hh_member,
                                                                 'total_ratio': total_ratio,
                                                                 'categories': categories,
                                                                 'data': data
                                                                 })


def getDistrictsHP2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id,division, Count(distinct household_id) total_hh_form_submission, SUM(hh_member) total_hh_member, case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2)  end AS ratio FROM f GROUP BY div_id,division"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_hh_form_submission = 0
        for each in df.total_hh_form_submission.tolist():
            total_hh_form_submission = total_hh_form_submission + each
        total_hh_member = 0
        for each in df.total_hh_member.tolist():
            total_hh_member = total_hh_member + each
        if total_hh_member != 0:
            total_ratio = (float(total_hh_form_submission) / float(total_hh_member))
        else:
            total_ratio = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_hh_form_submission': total_hh_form_submission,
                                        'total_hh_member': total_hh_member,
                                        'total_ratio': total_ratio}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id,district, Count(distinct household_id) total_hh_form_submission, SUM(hh_member) total_hh_member, case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2)  end AS ratio FROM f where div_id = " + str(
        division) + " GROUP BY dist_id,district"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_hh_form_submission = 0
    for each in df.total_hh_form_submission.tolist():
        total_hh_form_submission = total_hh_form_submission + each
    total_hh_member = 0
    for each in df.total_hh_member.tolist():
        total_hh_member = total_hh_member + each
    if total_hh_member != 0:
        total_ratio = (float(total_hh_form_submission) / float(total_hh_member))
    else:
        total_ratio = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'district_data': district_data,
                                    'total_hh_form_submission': total_hh_form_submission,
                                    'total_hh_member': total_hh_member,
                                    'total_ratio': total_ratio}))


def getUsersHP2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id,district, Count(distinct household_id) total_hh_form_submission, SUM(hh_member) total_hh_member, case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2)  end AS ratio FROM f where div_id = " + str(
            request.POST.get('div')) + " GROUP BY dist_id,district"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_hh_form_submission = 0
        for each in df.total_hh_form_submission.tolist():
            total_hh_form_submission = total_hh_form_submission + each
        total_hh_member = 0
        for each in df.total_hh_member.tolist():
            total_hh_member = total_hh_member + each
        if total_hh_member != 0:
            total_ratio = (float(total_hh_form_submission) / float(total_hh_member))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_hh_form_submission': total_hh_form_submission,
                                        'total_hh_member': total_hh_member,
                                        'total_ratio': total_ratio}))
    else:
        # query returns user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), Count(distinct household_id) total_hh_form_submission, SUM(hh_member) total_hh_member, case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2)  end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_hh_form_submission = 0
        for each in df.total_hh_form_submission.tolist():
            total_hh_form_submission = total_hh_form_submission + each
        total_hh_member = 0
        for each in df.total_hh_member.tolist():
            total_hh_member = total_hh_member + each
        if total_hh_member != 0:
            total_ratio = (float(total_hh_form_submission) / float(total_hh_member))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_hh_form_submission': total_hh_form_submission,
                                        'total_hh_member': total_hh_member,
                                        'total_ratio': total_ratio}))


def getDistrictsChartsHP2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where div_id = " + str(
        division) + " GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsHP2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]
    # get district id
    district = request.POST.get('dist')
    print(district)
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where div_id = " + str(
            request.POST.get('div')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataHP2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            request.POST.get('dist')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where userid = " + str(
            user_id) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))


def householdprofile3(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # for setting division_id and division_name
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, ( group_member ->> 'group_member/Age_year' ) age_year FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT distinct div_id,division FROM f"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # for intial chart data load
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, ( group_member ->> 'group_member/Age_year' ) age_year FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ag as (select count(*) as total_people from f), af as (SELECT age_year, Count(age_year) total_age_people ,total_people FROM f , ag GROUP BY age_year,total_people ORDER BY age_year) select age_year::int,round((total_age_people::float/total_people::float)::numeric * 100,2) as percentage from af"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['age_year'].tolist(), default=decimal_date_default)
    data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/householdprofile3.html', {'division': division,
                                                                 'categories': categories,
                                                                 'data': data
                                                                 })


def getDistrictsChartsHP3(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, ( group_member ->> 'group_member/Age_year' ) age_year FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT distinct dist_id,district FROM f where div_id = " + str(
        division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, ( group_member ->> 'group_member/Age_year' ) age_year FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ag AS (SELECT Count(*) AS total_people FROM f), af AS (SELECT age_year, div_id, Count(age_year) total_age_people, total_people FROM f, ag GROUP BY age_year, total_people,div_id ORDER BY age_year) SELECT age_year :: INT, Round(( total_age_people :: FLOAT / total_people :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM af where div_id =" + str(
        division)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['age_year'].tolist(), default=decimal_date_default)
    data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsHP3(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, ( group_member ->> 'group_member/Age_year' ) age_year FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ag AS (SELECT Count(*) AS total_people FROM f), af AS (SELECT age_year, div_id, Count(age_year) total_age_people, total_people FROM f, ag GROUP BY age_year, total_people,div_id ORDER BY age_year) SELECT age_year :: INT, Round(( total_age_people :: FLOAT / total_people :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM af where div_id =" + str(
            request.POST.get('div'))
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['age_year'].tolist(), default=decimal_date_default)
        data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, ( group_member ->> 'group_member/Age_year' ) age_year FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ag AS (SELECT Count(*) AS total_people FROM f), af AS (SELECT age_year, dist_id, Count(age_year) total_age_people, total_people FROM f, ag GROUP BY age_year, total_people,dist_id ORDER BY age_year) SELECT age_year :: INT, Round(( total_age_people :: FLOAT / total_people :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM af where dist_id =" + str(
            district)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['age_year'].tolist(), default=decimal_date_default)
        data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataHP3(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, ( group_member ->> 'group_member/Age_year' ) age_year FROM t), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ag AS (SELECT Count(*) AS total_people FROM f), af AS (SELECT age_year, dist_id, Count(age_year) total_age_people, total_people FROM f, ag GROUP BY age_year, total_people,dist_id ORDER BY age_year) SELECT age_year :: INT, Round(( total_age_people :: FLOAT / total_people :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM af where dist_id =" + str(
            request.POST.get('dist'))

        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['age_year'].tolist(), default=decimal_date_default)
        data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, ( group_member ->> 'group_member/Age_year' ) age_year FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ag AS (SELECT Count(*) AS total_people FROM f), af AS (SELECT age_year, userid, Count(age_year) total_age_people, total_people FROM f, ag GROUP BY age_year, total_people,userid ORDER BY age_year) SELECT age_year :: INT, Round(( total_age_people :: FLOAT / total_people :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM af where userid =" + (
                    user_id)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['age_year'].tolist(), default=decimal_date_default)
        data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))


def birthregistration(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_hh_form_submission,total_hh_member,ratio
    # data stored in division_data

    query = "with t as( SELECT id, user_id, date_created::date , Json_array_elements(( json ->> 'group_member') :: json) group_member,( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS null ), h as(select user_id,date_created,household_id from t where (group_member->>'group_member/Age_Month_Calculation')::int between 0 and 11), f as( select h.*,vw_geo_data_catchment.* from h inner join vw_geo_data_catchment on h.user_id=vw_geo_data_catchment.user_id ) select div_id,division,count(*) total_children,count(distinct household_id) total_hh, round((count(*)::float/count(distinct household_id)::float)::numeric,2) as ratio from f group by div_id,division"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)

    # for setting division_id and division_name
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # total ratio calculation
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_children) / float(total_hh))
    else:
        total_ratio = 0

    # for intial chart data load
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 11), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) ::NUMERIC,2) end AS ratio FROM f GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/birth_registration.html', {'division': division,
                                                                  'divison_data': divison_data,
                                                                  'total_children': total_children,
                                                                  'total_hh': total_hh,
                                                                  'total_ratio': total_ratio,
                                                                  'categories': categories,
                                                                  'data': data
                                                                  })


def getDistrictsBR(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "with t as( SELECT id, user_id, date_created::date , Json_array_elements(( json ->> 'group_member') :: json) group_member,( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS null ), h as(select user_id,date_created,household_id from t where (group_member->>'group_member/Age_Month_Calculation')::int between 0 and 11), f as( select h.*,vw_geo_data_catchment.* from h inner join vw_geo_data_catchment on h.user_id=vw_geo_data_catchment.user_id ) select div_id,division,count(*) total_children,count(distinct household_id) total_hh, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) ::NUMERIC,2) end AS ratio from f group by div_id,division"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_children) / float(total_hh))
        else:
            total_ratio = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_children': total_children,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "with t as( SELECT id, user_id, date_created::date , Json_array_elements(( json ->> 'group_member') :: json) group_member,( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS null ), h as(select user_id,date_created,household_id from t where (group_member->>'group_member/Age_Month_Calculation')::int between 0 and 11), f as( select h.*,vw_geo_data_catchment.* from h inner join vw_geo_data_catchment on h.user_id=vw_geo_data_catchment.user_id ) select dist_id,district,count(*) total_children,count(distinct household_id) total_hh, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) ::NUMERIC,2) end AS ratio from f where div_id =" + str(
        division) + " group by dist_id,district"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_children) / float(total_hh))
    else:
        total_ratio = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'district_data': district_data,
                                    'total_children': total_children,
                                    'total_hh': total_hh,
                                    'total_ratio': total_ratio}))


def getUsersBR(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "with t as( SELECT id, user_id, date_created::date , Json_array_elements(( json ->> 'group_member') :: json) group_member,( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS null ), h as(select user_id,date_created,household_id from t where (group_member->>'group_member/Age_Month_Calculation')::int between 0 and 11), f as( select h.*,vw_geo_data_catchment.* from h inner join vw_geo_data_catchment on h.user_id=vw_geo_data_catchment.user_id ) select dist_id,district,count(*) total_children,count(distinct household_id) total_hh, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) ::NUMERIC,2) end AS ratio from f where div_id =" + str(
            request.POST.get('div')) + " group by dist_id,district"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_children) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_children': total_children,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))
    else:
        # query returns user wise data
        query = "with t as( SELECT id, user_id, date_created::date , Json_array_elements(( json ->> 'group_member') :: json) group_member,( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS null ), h as(select user_id,date_created,household_id from t where (group_member->>'group_member/Age_Month_Calculation')::int between 0 and 11), f as( select h.*,vw_geo_data_catchment.*,h.user_id as userid from h inner join vw_geo_data_catchment on h.user_id=vw_geo_data_catchment.user_id ) select userid,(select username from auth_user where id = userid),count(*) total_children,count(distinct household_id) total_hh, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) ::NUMERIC,2) end AS ratio from f where dist_id = " + str(
            district) + " group by userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_children) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_children': total_children,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))


def getDistrictsChartsBR(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 11), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) ::NUMERIC,2) end AS ratio FROM f where div_id = " + str(
        division) + " GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsBR(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]
    # get district id
    district = request.POST.get('dist')
    print(district)
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 11), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) ::NUMERIC,2) end AS ratio FROM f where div_id = " + str(
            request.POST.get('div')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 11), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) ::NUMERIC,2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataBR(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 11), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) ::NUMERIC,2) end AS ratio FROM f where dist_id = " + str(
            request.POST.get('dist')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 11), f AS (SELECT h.*, vw_geo_data_catchment.*,h.user_id as userid FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) ::NUMERIC,2) end AS ratio FROM f where userid = " + str(
            user_id) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))


def nutrition1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_hh_form_submission,total_hh_member,ratio
    # data stored in division_data

    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id,division, Count(*) total_women, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY div_id,division"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)

    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    # total ratio calculation
    total_women = 0
    for each in df.total_women.tolist():
        total_women = total_women + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_women) / float(total_hh))
    else:
        total_ratio = 0

    # for setting division_id and division_name
    query = "select distinct div_id,division from vw_geo_data_catchment "
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # for intial chart data load
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/nutrition1.html', {'division': division,
                                                          'divison_data': divison_data,
                                                          'total_women': total_women,
                                                          'total_hh': total_hh,
                                                          'total_ratio': total_ratio,
                                                          'categories': categories,
                                                          'data': data
                                                          })


def getDistrictsNT1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id,division, Count(*) total_women, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY div_id,division"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_women = 0
        for each in df.total_women.tolist():
            total_women = total_women + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_women) / float(total_hh))
        else:
            total_ratio = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_women': total_women,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id,district, Count(*) total_women, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
        division) + " group by dist_id,district"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_women = 0
    for each in df.total_women.tolist():
        total_women = total_women + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_women) / float(total_hh))
    else:
        total_ratio = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'district_data': district_data,
                                    'total_women': total_women,
                                    'total_hh': total_hh,
                                    'total_ratio': total_ratio}))


def getUsersNT1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id,district, Count(*) total_women, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
            request.POST.get('div')) + " group by dist_id,district"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_women = 0
        for each in df.total_women.tolist():
            total_women = total_women + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_women) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_women': total_women,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))
    else:
        # query returns user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 1), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), Count(*) total_women, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_women = 0
        for each in df.total_women.tolist():
            total_women = total_women + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_women) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_women': total_women,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))


def getDistrictsChartsNT1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
        division) + " GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsNT1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]
    # get district id
    district = request.POST.get('dist')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
            request.POST.get('div')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY date_created"

        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataNT1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where dist_id = " + str(
            request.POST.get('dist')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.*,h.user_id as userid FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where userid = " + str(
            user_id) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))


def nutrition2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_hh_form_submission,total_hh_member,ratio
    # data stored in division_data

    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Pregnant_lactating' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id,division, Count(*) total_women, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY div_id,division"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)

    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    # total ratio calculation
    total_women = 0
    for each in df.total_women.tolist():
        total_women = total_women + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_women) / float(total_hh))
    else:
        total_ratio = 0

    # for setting division_id and division_name
    query = "select distinct div_id,division from vw_geo_data_catchment "
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # for intial chart data load
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Pregnant_lactating' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/nutrition2.html', {'division': division,
                                                          'divison_data': divison_data,
                                                          'total_women': total_women,
                                                          'total_hh': total_hh,
                                                          'total_ratio': total_ratio,
                                                          'categories': categories,
                                                          'data': data
                                                          })


def getDistrictsNT2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Pregnant_lactating' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id,division, Count(*) total_women, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY div_id,division"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_women = 0
        for each in df.total_women.tolist():
            total_women = total_women + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_women) / float(total_hh))
        else:
            total_ratio = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_women': total_women,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Pregnant_lactating' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id,district, Count(*) total_women, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
        division) + " group by dist_id,district"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_women = 0
    for each in df.total_women.tolist():
        total_women = total_women + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_women) / float(total_hh))
    else:
        total_ratio = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'district_data': district_data,
                                    'total_women': total_women,
                                    'total_hh': total_hh,
                                    'total_ratio': total_ratio}))


def getUsersNT2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Pregnant_lactating' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id,district, Count(*) total_women, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
            request.POST.get('div')) + " group by dist_id,district"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_women = 0
        for each in df.total_women.tolist():
            total_women = total_women + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_women) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_women': total_women,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))
    else:
        # query returns user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Pregnant_lactating' ) :: INT = 1), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), Count(*) total_women, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_women = 0
        for each in df.total_women.tolist():
            total_women = total_women + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_women) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_women': total_women,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))


def getDistrictsChartsNT2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Pregnant_lactating' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
        division) + " GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsNT2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]
    # get district id
    district = request.POST.get('dist')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Pregnant_lactating' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
            request.POST.get('div')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Pregnant_lactating' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY date_created"

        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataNT2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Pregnant_lactating' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where dist_id = " + str(
            request.POST.get('dist')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Pregnant_lactating' ) :: INT = 1), f AS (SELECT h.*, vw_geo_data_catchment.*,h.user_id as userid FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where userid = " + str(
            user_id) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))


def nutrition3(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_hh_form_submission,total_hh_member,ratio
    # data stored in division_data

    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 0 and 5), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id,division, Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY div_id,division"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)

    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    # total ratio calculation
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_children) / float(total_hh))
    else:
        total_ratio = 0

    # for setting division_id and division_name
    query = "select distinct div_id,division from vw_geo_data_catchment "
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # for intial chart data load
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 0 and 5), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/nutrition3.html', {'division': division,
                                                          'divison_data': divison_data,
                                                          'total_children': total_children,
                                                          'total_hh': total_hh,
                                                          'total_ratio': total_ratio,
                                                          'categories': categories,
                                                          'data': data
                                                          })


def getDistrictsNT3(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 0 and 5), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id,division, Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY div_id,division"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_children) / float(total_hh))
        else:
            total_ratio = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_children': total_children,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 0 and 5), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id,district, Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
        division) + " group by dist_id,district"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_children) / float(total_hh))
    else:
        total_ratio = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'district_data': district_data,
                                    'total_children': total_children,
                                    'total_hh': total_hh,
                                    'total_ratio': total_ratio}))


def getUsersNT3(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 0 and 5), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id,district, Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
            request.POST.get('div')) + " group by dist_id,district"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_children) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_children': total_children,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))
    else:
        # query returns user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 0 and 5), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_children) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_children': total_children,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))


def getDistrictsChartsNT3(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 0 and 5), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
        division) + " GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsNT3(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]
    # get district id
    district = request.POST.get('dist')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 0 and 5), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
            request.POST.get('div')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 0 and 5), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY date_created"

        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataNT3(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 0 and 5), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where dist_id = " + str(
            request.POST.get('dist')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 0 and 5), f AS (SELECT h.*, vw_geo_data_catchment.*,h.user_id as userid FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where userid = " + str(
            user_id) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))


def nutrition4(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_hh_form_submission,total_hh_member,ratio
    # data stored in division_data

    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 6 and 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id,division, Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY div_id,division"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)



    # total ratio calculation
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_children) / float(total_hh))
    else:
        total_ratio = 0

    # for setting division_id and division_name
    query = "select distinct div_id,division from vw_geo_data_catchment "
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # for intial chart data load
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 6 and 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/nutrition4.html', {'division': division,
                                                          'divison_data': divison_data,
                                                          'total_children': total_children,
                                                          'total_hh': total_hh,
                                                          'total_ratio': total_ratio,
                                                          'categories': categories,
                                                          'data': data
                                                          })


def getDistrictsNT4(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 6 and 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id,division, Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY div_id,division"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_children) / float(total_hh))
        else:
            total_ratio = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_children': total_children,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 6 and 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id,district, Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
        division) + " group by dist_id,district"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_children) / float(total_hh))
    else:
        total_ratio = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'district_data': district_data,
                                    'total_children': total_children,
                                    'total_hh': total_hh,
                                    'total_ratio': total_ratio}))


def getUsersNT4(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 6 and 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id,district, Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
            request.POST.get('div')) + " group by dist_id,district"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_children) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_children': total_children,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))
    else:
        # query returns user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 6 and 23), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_children) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_children': total_children,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))


def getDistrictsChartsNT4(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 6 and 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
        division) + " GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsNT4(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]
    # get district id
    district = request.POST.get('dist')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 6 and 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
            request.POST.get('div')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 6 and 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY date_created"

        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataNT4(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 6 and 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where dist_id = " + str(
            request.POST.get('dist')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT between 6 and 23), f AS (SELECT h.*, vw_geo_data_catchment.*,h.user_id as userid FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where userid = " + str(
            user_id) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))


def nutrition5(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching early_bf form id
    query = "select id from logger_xform where id_string = 'early_bf'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    ebf_form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_hh_form_submission,total_hh_member,ratio
    # data stored in division_data

    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'ever_breastfed' ) brestfed FROM public.logger_instance WHERE xform_id = " + str(
        ebf_form_id) + " AND deleted_at IS NULL AND ( json ->> 'ever_breastfed' ) :: INT = 1), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 23), ebf1 AS (SELECT ebf.*, vw_geo_data_catchment.* FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT division, div_id, Count(*) total_breastfed FROM ebf1 GROUP BY division, div_id) SELECT f.division, f.div_id, Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_breastfed, Round(( total_breastfed :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.div_id = f.div_id GROUP BY f.division, total_breastfed, f.div_id"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)

    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    # total ratio calculation
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each

    total_household = 0
    for each in df.total_household.tolist():
        total_household = total_household + each
    if total_household != 0:
        total_ratio = (float(total_children) / float(total_household))
    else:
        total_ratio = 0

    total_breastfed = 0
    for each in df.total_breastfed.tolist():
        total_breastfed = total_breastfed + each

    if total_children != 0:
        total_percentage = (float(total_breastfed) / float(total_children))
    else:
        total_percentage = 0


    # for setting division_id and division_name
    query = "select distinct div_id,division from vw_geo_data_catchment "
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # for intial chart data load
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio FROM f GROUP by date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/nutrition5.html', {'division': division,
                                                          'divison_data': divison_data,
                                                          'total_children': total_children,
                                                          'total_household': total_household,
                                                          'total_ratio': total_ratio,
                                                          'total_breastfed': total_breastfed,
                                                          'total_percentage': total_percentage,
                                                          'categories': categories,
                                                          'data': data
                                                          })


def getDistrictsNT5(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching early_bf form id
    query = "select id from logger_xform where id_string = 'early_bf'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    ebf_form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'ever_breastfed' ) brestfed FROM public.logger_instance WHERE xform_id = " + str(
            ebf_form_id) + " AND deleted_at IS NULL AND ( json ->> 'ever_breastfed' ) :: INT = 1), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 23), ebf1 AS (SELECT ebf.*, vw_geo_data_catchment.* FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT division, div_id, Count(*) total_breastfed FROM ebf1 GROUP BY division, div_id) SELECT f.division, f.div_id, Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_breastfed, Round(( total_breastfed :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.div_id = f.div_id GROUP BY f.division, total_breastfed, f.div_id"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each

        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        if total_household != 0:
            total_ratio = (float(total_children) / float(total_household))
        else:
            total_ratio = 0

        total_breastfed = 0
        for each in df.total_breastfed.tolist():
            total_breastfed = total_breastfed + each

        if total_children != 0:
            total_percentage = (float(total_breastfed) / float(total_children))
        else:
            total_percentage = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_children': total_children,
                                        'total_household': total_household,
                                        'total_ratio': total_ratio,
                                        'total_breastfed': total_breastfed,
                                        'total_percentage': total_percentage}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'ever_breastfed' ) brestfed FROM public.logger_instance WHERE xform_id = "+str(ebf_form_id)+" AND deleted_at IS NULL AND ( json ->> 'ever_breastfed' ) :: INT = 1), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 23), ebf1 AS (SELECT ebf.*, vw_geo_data_catchment.* FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT district, dist_id, Count(*) total_breastfed FROM ebf1 where div_id = "+str(division)+" GROUP BY district, dist_id) SELECT f.district, f.dist_id, Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_breastfed, Round(( total_breastfed :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.dist_id = f.dist_id GROUP BY f.district, total_breastfed, f.dist_id"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each

    total_household = 0
    for each in df.total_household.tolist():
        total_household = total_household + each
    if total_household != 0:
        total_ratio = (float(total_children) / float(total_household))
    else:
        total_ratio = 0

    total_breastfed = 0
    for each in df.total_breastfed.tolist():
        total_breastfed = total_breastfed + each

    if total_children != 0:
        total_percentage = (float(total_breastfed) / float(total_children))
    else:
        total_percentage = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'total_children': total_children,
                                    'total_household': total_household,
                                    'total_ratio': total_ratio,
                                    'total_breastfed': total_breastfed,
                                    'total_percentage': total_percentage}))


def getUsersNT5(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching early_bf form id
    query = "select id from logger_xform where id_string = 'early_bf'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    ebf_form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = ""
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'ever_breastfed' ) brestfed FROM public.logger_instance WHERE xform_id = " + str(
            ebf_form_id) + " AND deleted_at IS NULL AND ( json ->> 'ever_breastfed' ) :: INT = 1), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 23), ebf1 AS (SELECT ebf.*, vw_geo_data_catchment.* FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT district, dist_id, Count(*) total_breastfed FROM ebf1 where div_id = " + str(
            request.POST.get('div')) + " GROUP BY district, dist_id) SELECT f.district, f.dist_id, Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_breastfed, Round(( total_breastfed :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.dist_id = f.dist_id GROUP BY f.district, total_breastfed, f.dist_id"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each

        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        if total_household != 0:
            total_ratio = (float(total_children) / float(total_household))
        else:
            total_ratio = 0

        total_breastfed = 0
        for each in df.total_breastfed.tolist():
            total_breastfed = total_breastfed + each

        if total_children != 0:
            total_percentage = (float(total_breastfed) / float(total_children))
        else:
            total_percentage = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_children': total_children,
                                        'total_household': total_household,
                                        'total_ratio': total_ratio,
                                        'total_breastfed': total_breastfed,
                                        'total_percentage': total_percentage}))
    else:
        # query returns user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'ever_breastfed' ) brestfed FROM public.logger_instance WHERE xform_id = "+str(ebf_form_id)+" AND deleted_at IS NULL AND ( json ->> 'ever_breastfed' ) :: INT = 1), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 23), ebf1 AS (SELECT ebf.*,ebf.user_id as userid, vw_geo_data_catchment.* FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*, h.user_id as user__id, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT userid, Count(*) total_breastfed FROM ebf1 where ebf1.dist_id = "+str(district)+" GROUP BY userid) SELECT f.user__id, (select username from auth_user where id = user__id), Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_breastfed, Round(( total_breastfed :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.userid = f.user__id GROUP BY total_breastfed, f.user__id"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each

        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        if total_household != 0:
            total_ratio = (float(total_children) / float(total_household))
        else:
            total_ratio = 0

        total_breastfed = 0
        for each in df.total_breastfed.tolist():
            total_breastfed = total_breastfed + each

        if total_children != 0:
            total_percentage = (float(total_breastfed) / float(total_children))
        else:
            total_percentage = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_children': total_children,
                                        'total_household': total_household,
                                        'total_ratio': total_ratio,
                                        'total_breastfed': total_breastfed,
                                        'total_percentage': total_percentage}))

def getDistrictsChartsNT5(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio FROM f where div_id = "+str(division)+" GROUP by date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsNT5(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]
    # get district id
    district = request.POST.get('dist')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio FROM f where div_id = " + str(
            request.POST.get('div')) + " GROUP by date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio FROM f where dist_id = " + str(
            district) + " GROUP by date_created"

        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataNT5(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 23), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio FROM f where dist_id = " + str(
            request.POST.get('dist')) + " GROUP by date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 23), f AS (SELECT h.*, vw_geo_data_catchment.*,h.user_id as userid FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio FROM f where userid = " + str(
            user_id) + " GROUP by date_created"

        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))


def education1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_hh_form_submission,total_hh_member,ratio
    # data stored in division_data

    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 59), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id, division, Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY div_id, division"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)

    # total ratio calculation
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_children) / float(total_hh))
    else:
        total_ratio = 0

    # for setting division_id and division_name
    query = "select distinct div_id,division from vw_geo_data_catchment "
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # for intial chart data load
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 59), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/education1.html', {'division': division,
                                                          'divison_data': divison_data,
                                                          'total_children': total_children,
                                                          'total_hh': total_hh,
                                                          'total_ratio': total_ratio,
                                                          'categories': categories,
                                                          'data': data
                                                          })


def getDistrictsED1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 59), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id, division, Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY div_id, division"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_children) / float(total_hh))
        else:
            total_ratio = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_children': total_children,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 59), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id, district, Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = "+str(division)+" GROUP BY dist_id, district"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_children) / float(total_hh))
    else:
        total_ratio = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'district_data': district_data,
                                    'total_children': total_children,
                                    'total_hh': total_hh,
                                    'total_ratio': total_ratio}))


def getUsersED1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 59), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id, district, Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where div_id = " + str(
            request.POST.get('div')) + " GROUP BY dist_id, district"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_children) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_children': total_children,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))
    else:
        # query returns user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 59), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid, (select username from auth_user where id = userid), Count(*) total_children, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where dist_id = "+str(district)+" GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_children) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_children': total_children,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))


def getDistrictsChartsED1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
        form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 59), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f  where div_id = " + str(
        division) + " GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsED1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]
    # get district id
    district = request.POST.get('dist')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 59), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f  where div_id = " + str(
            request.POST.get('div')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 59), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f  where dist_id = " + str(
            district) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataED1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 59), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f  where dist_id = " + str(
            request.POST.get('dist')) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_Month_Calculation' ) :: INT BETWEEN 0 AND 59), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f where userid = " + str(
            user_id) + " GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))


def education2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching attending_edu_institution form id
    query = "select id from public.logger_xform where id_string = 'attending_edu_institution'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    aei_form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_hh_form_submission,total_hh_member,ratio
    # data stored in division_data

    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'attending_school' ) attending_school FROM public.logger_instance WHERE xform_id = "+str(aei_form_id)+" AND deleted_at IS NULL AND ( json ->> 'attending_school' ) :: INT = 1) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 5 AND 14), ebf1 AS (SELECT ebf.*, vw_geo_data_catchment.* FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT division, div_id, Count(*) total_school_going_children FROM ebf1 GROUP BY division, div_id) SELECT f.division, f.div_id, Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_school_going_children, Round(( total_school_going_children :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.div_id = f.div_id GROUP BY f.division, total_school_going_children, f.div_id"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)

    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    # total ratio calculation
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each

    total_household = 0
    for each in df.total_household.tolist():
        total_household = total_household + each
    if total_household != 0:
        total_ratio = (float(total_children) / float(total_household))
    else:
        total_ratio = 0

    total_school_going_children = 0
    for each in df.total_school_going_children.tolist():
        total_school_going_children = total_school_going_children + each

    if total_children != 0:
        total_percentage = (float(total_school_going_children) / float(total_children))
    else:
        total_percentage = 0


    # for setting division_id and division_name
    query = "select distinct div_id,division from vw_geo_data_catchment "
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # for intial chart data load
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 5 AND 14), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/education2.html', {'division': division,
                                                          'divison_data': divison_data,
                                                          'total_children': total_children,
                                                          'total_household': total_household,
                                                          'total_ratio': total_ratio,
                                                          'total_school_going_children': total_school_going_children,
                                                          'total_percentage': total_percentage,
                                                          'categories': categories,
                                                          'data': data
                                                          })

def getDistrictsED2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching attending_edu_institution form id
    query = "select id from logger_xform where id_string = 'attending_edu_institution'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    aei_form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'attending_school' ) attending_school FROM public.logger_instance WHERE xform_id = "+str(aei_form_id)+" AND deleted_at IS NULL AND ( json ->> 'attending_school' ) :: INT = 1) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 5 AND 14), ebf1 AS (SELECT ebf.*, vw_geo_data_catchment.* FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT division, div_id, Count(*) total_school_going_children FROM ebf1 GROUP BY division, div_id) SELECT f.division, f.div_id, Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_school_going_children, Round(( total_school_going_children :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.div_id = f.div_id GROUP BY f.division, total_school_going_children, f.div_id"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each

        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        if total_household != 0:
            total_ratio = (float(total_children) / float(total_household))
        else:
            total_ratio = 0

        total_school_going_children = 0
        for each in df.total_school_going_children.tolist():
            total_school_going_children = total_school_going_children + each

        if total_children != 0:
            total_percentage = (float(total_school_going_children) / float(total_children))
        else:
            total_percentage = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_children': total_children,
                                        'total_household': total_household,
                                        'total_ratio': total_ratio,
                                        'total_school_going_children': total_school_going_children,
                                        'total_percentage': total_percentage}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'attending_school' ) attending_school FROM public.logger_instance WHERE xform_id = "+str(aei_form_id)+" AND deleted_at IS NULL AND ( json ->> 'attending_school' ) :: INT = 1) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 5 AND 14), ebf1 AS (SELECT ebf.*, vw_geo_data_catchment.* FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT ebf1.dist_id, ebf1.district, Count(*) total_school_going_children FROM ebf1 where ebf1.div_id = "+str(division)+" GROUP BY ebf1.dist_id, ebf1.district) SELECT f.district, f.dist_id, Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_school_going_children, Round(( total_school_going_children :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.dist_id = f.dist_id GROUP BY f.dist_id, f.district, total_school_going_children"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each

    total_household = 0
    for each in df.total_household.tolist():
        total_household = total_household + each
    if total_household != 0:
        total_ratio = (float(total_children) / float(total_household))
    else:
        total_ratio = 0

    total_school_going_children = 0
    for each in df.total_school_going_children.tolist():
        total_school_going_children = total_school_going_children + each

    if total_children != 0:
        total_percentage = (float(total_school_going_children) / float(total_children))
    else:
        total_percentage = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'total_children': total_children,
                                    'total_household': total_household,
                                    'total_ratio': total_ratio,
                                    'total_school_going_children': total_school_going_children,
                                    'total_percentage': total_percentage}))


def getUsersED2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching attending_edu_institution form id
    query = "select id from logger_xform where id_string = 'attending_edu_institution'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    aei_form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'attending_school' ) attending_school FROM public.logger_instance WHERE xform_id = " + str(
            aei_form_id) + " AND deleted_at IS NULL AND ( json ->> 'attending_school' ) :: INT = 1) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 5 AND 14), ebf1 AS (SELECT ebf.*, vw_geo_data_catchment.* FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT ebf1.dist_id, ebf1.district, Count(*) total_school_going_children FROM ebf1 where ebf1.div_id = " + str(
            request.POST.get('div')) + " GROUP BY ebf1.dist_id, ebf1.district) SELECT f.district, f.dist_id, Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_school_going_children, Round(( total_school_going_children :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.dist_id = f.dist_id GROUP BY f.dist_id, f.district, total_school_going_children"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each

        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        if total_household != 0:
            total_ratio = (float(total_children) / float(total_household))
        else:
            total_ratio = 0

        total_school_going_children = 0
        for each in df.total_school_going_children.tolist():
            total_school_going_children = total_school_going_children + each

        if total_children != 0:
            total_percentage = (float(total_school_going_children) / float(total_children))
        else:
            total_percentage = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_children': total_children,
                                        'total_household': total_household,
                                        'total_ratio': total_ratio,
                                        'total_school_going_children': total_school_going_children,
                                        'total_percentage': total_percentage}))
    else:
        # query returns user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'attending_school' ) attending_school FROM public.logger_instance WHERE xform_id = "+str(aei_form_id)+" AND deleted_at IS NULL AND ( json ->> 'attending_school' ) :: INT = 1) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 5 AND 14), ebf1 AS (SELECT ebf.*,ebf.user_id as userid, vw_geo_data_catchment.* FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT ebf1.userid, Count(*) total_school_going_children FROM ebf1 where ebf1.dist_id = "+str(district)+" GROUP BY ebf1.userid) SELECT f.userid, (select username from auth_user where id = f.userid), Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_school_going_children, Round(( total_school_going_children :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.userid = f.userid GROUP BY f.userid, total_school_going_children"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each

        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        if total_household != 0:
            total_ratio = (float(total_children) / float(total_household))
        else:
            total_ratio = 0

        total_school_going_children = 0
        for each in df.total_school_going_children.tolist():
            total_school_going_children = total_school_going_children + each

        if total_children != 0:
            total_percentage = (float(total_school_going_children) / float(total_children))
        else:
            total_percentage = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_children': total_children,
                                        'total_household': total_household,
                                        'total_ratio': total_ratio,
                                        'total_school_going_children': total_school_going_children,
                                        'total_percentage': total_percentage}))

def getDistrictsChartsED2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 5 AND 14), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where div_id = "+str(division)+" GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsED2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]
    # get district id
    district = request.POST.get('dist')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 5 AND 14), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where div_id = "+str(request.POST.get('div'))+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 5 AND 14), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = "+str(district)+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataED2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 5 AND 14), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = "+str(request.POST.get('dist'))+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 5 AND 14), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where userid = "+str(user_id)+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))


def health1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching adolescent_health form id
    query = "select id from public.logger_xform where id_string = 'adolescent_health'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    ah_form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_hh_form_submission,total_hh_member,ratio
    # data stored in division_data
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'visited_any_health_facility' ) visited_any_health_facility FROM public.logger_instance WHERE xform_id = "+str(ah_form_id)+" AND deleted_at IS NULL AND ( json ->> 'visited_any_health_facility' ) :: INT = 1) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 10 AND 19), ebf1 AS (SELECT ebf.*, vw_geo_data_catchment.div_id, vw_geo_data_catchment.division, vw_geo_data_catchment.district FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*, vw_geo_data_catchment.div_id, vw_geo_data_catchment.division, vw_geo_data_catchment.district FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT division, div_id, Count(*) total_facility_visited_children FROM ebf1 GROUP BY division, div_id) SELECT f.division, f.div_id, Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_facility_visited_children, Round(( total_facility_visited_children :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.div_id = f.div_id GROUP BY f.division, total_facility_visited_children, f.div_id"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    # total ratio calculation
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each

    total_household = 0
    for each in df.total_household.tolist():
        total_household = total_household + each
    if total_household != 0:
        total_ratio = (float(total_children) / float(total_household))
    else:
        total_ratio = 0

    total_facility_visited_children = 0
    for each in df.total_facility_visited_children.tolist():
        total_facility_visited_children = total_facility_visited_children + each

    if total_children != 0:
        total_percentage = (float(total_facility_visited_children) / float(total_children))
    else:
        total_percentage = 0


    # for setting division_id and division_name
    query = "select distinct div_id,division from vw_geo_data_catchment "
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # for intial chart data load
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 10 AND 19), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/health1.html', {'division': division,
                                                          'divison_data': divison_data,
                                                          'total_children': total_children,
                                                          'total_household': total_household,
                                                          'total_ratio': total_ratio,
                                                          'total_facility_visited_children': total_facility_visited_children,
                                                          'total_percentage': total_percentage,
                                                          'categories': categories,
                                                          'data': data
                                                          })

def getDistrictsHL1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching adolescent_health form id
    query = "select id from logger_xform where id_string = 'adolescent_health'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    ah_form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'visited_any_health_facility' ) visited_any_health_facility FROM public.logger_instance WHERE xform_id = "+str(ah_form_id)+" AND deleted_at IS NULL AND ( json ->> 'visited_any_health_facility' ) :: INT = 1) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 10 AND 19), ebf1 AS (SELECT ebf.*, vw_geo_data_catchment.div_id, vw_geo_data_catchment.division, vw_geo_data_catchment.district FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*, vw_geo_data_catchment.div_id, vw_geo_data_catchment.division, vw_geo_data_catchment.district FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT division, div_id, Count(*) total_facility_visited_children FROM ebf1 GROUP BY division, div_id) SELECT f.division, f.div_id, Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_facility_visited_children, Round(( total_facility_visited_children :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.div_id = f.div_id GROUP BY f.division, total_facility_visited_children, f.div_id"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each

        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        if total_household != 0:
            total_ratio = (float(total_children) / float(total_household))
        else:
            total_ratio = 0

        total_facility_visited_children = 0
        for each in df.total_facility_visited_children.tolist():
            total_facility_visited_children = total_facility_visited_children + each

        if total_children != 0:
            total_percentage = (float(total_facility_visited_children) / float(total_children))
        else:
            total_percentage = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_children': total_children,
                                        'total_household': total_household,
                                        'total_ratio': total_ratio,
                                        'total_facility_visited_children': total_facility_visited_children,
                                        'total_percentage': total_percentage}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'visited_any_health_facility' ) visited_any_health_facility FROM public.logger_instance WHERE xform_id = "+str(ah_form_id)+" AND deleted_at IS NULL AND ( json ->> 'visited_any_health_facility' ) :: INT = 1) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 10 AND 19), ebf1 AS (SELECT ebf.*, vw_geo_data_catchment.* FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT district, dist_id, Count(*) total_facility_visited_children FROM ebf1 where div_id = "+str(division)+" GROUP BY district, dist_id) SELECT f.district, f.dist_id, Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_facility_visited_children, Round(( total_facility_visited_children :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.dist_id = f.dist_id GROUP BY f.district, total_facility_visited_children, f.dist_id"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_children = 0
    for each in df.total_children.tolist():
        total_children = total_children + each

    total_household = 0
    for each in df.total_household.tolist():
        total_household = total_household + each
    if total_household != 0:
        total_ratio = (float(total_children) / float(total_household))
    else:
        total_ratio = 0

    total_facility_visited_children = 0
    for each in df.total_facility_visited_children.tolist():
        total_facility_visited_children = total_facility_visited_children + each

    if total_children != 0:
        total_percentage = (float(total_facility_visited_children) / float(total_children))
    else:
        total_percentage = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'total_children': total_children,
                                    'total_household': total_household,
                                    'total_ratio': total_ratio,
                                    'total_facility_visited_children': total_facility_visited_children,
                                    'total_percentage': total_percentage}))


def getUsersHL1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching adolescent_health form id
    query = "select id from logger_xform where id_string = 'adolescent_health'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    ah_form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'visited_any_health_facility' ) visited_any_health_facility FROM public.logger_instance WHERE xform_id = "+str(ah_form_id)+" AND deleted_at IS NULL AND ( json ->> 'visited_any_health_facility' ) :: INT = 1) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 10 AND 19), ebf1 AS (SELECT ebf.*, vw_geo_data_catchment.* FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT district, dist_id, Count(*) total_facility_visited_children FROM ebf1 where div_id = "+str(request.POST.get('div'))+" GROUP BY district, dist_id) SELECT f.district, f.dist_id, Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_facility_visited_children, Round(( total_facility_visited_children :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.dist_id = f.dist_id GROUP BY f.district, total_facility_visited_children, f.dist_id"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each

        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        if total_household != 0:
            total_ratio = (float(total_children) / float(total_household))
        else:
            total_ratio = 0

        total_facility_visited_children = 0
        for each in df.total_facility_visited_children.tolist():
            total_facility_visited_children = total_facility_visited_children + each

        if total_children != 0:
            total_percentage = (float(total_facility_visited_children) / float(total_children))
        else:
            total_percentage = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_children': total_children,
                                        'total_household': total_household,
                                        'total_ratio': total_ratio,
                                        'total_facility_visited_children': total_facility_visited_children,
                                        'total_percentage': total_percentage}))
    else:
        # query returns user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS (SELECT id, user_id, date_created :: DATE, ( json ->> 'hh_id' ) household_id, ( json ->> 'visited_any_health_facility' ) visited_any_health_facility FROM public.logger_instance WHERE xform_id = "+str(ah_form_id)+" AND deleted_at IS NULL AND ( json ->> 'visited_any_health_facility' ) :: INT = 1) , h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 10 AND 19), ebf1 AS (SELECT ebf.*,ebf.user_id as userid, vw_geo_data_catchment.* FROM ebf inner join vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS (SELECT userid, Count(*) total_facility_visited_children FROM ebf1 where dist_id = "+str(district)+" GROUP BY userid) SELECT (select username from auth_user where id = f.userid), f.userid, Count(*) total_children, Count(DISTINCT household_id) total_household, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio, total_facility_visited_children, Round(( total_facility_visited_children :: FLOAT / Count(*) :: FLOAT ) :: NUMERIC * 100, 2) AS percentage FROM f, ebf2 WHERE ebf2.userid = f.userid GROUP BY f.userid, total_facility_visited_children"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_children = 0
        for each in df.total_children.tolist():
            total_children = total_children + each

        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        if total_household != 0:
            total_ratio = (float(total_children) / float(total_household))
        else:
            total_ratio = 0

        total_facility_visited_children = 0
        for each in df.total_facility_visited_children.tolist():
            total_facility_visited_children = total_facility_visited_children + each

        if total_children != 0:
            total_percentage = (float(total_facility_visited_children) / float(total_children))
        else:
            total_percentage = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_children': total_children,
                                        'total_household': total_household,
                                        'total_ratio': total_ratio,
                                        'total_facility_visited_children': total_facility_visited_children,
                                        'total_percentage': total_percentage}))

def getDistrictsChartsHL1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 10 AND 19), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where div_id = "+str(division)+" GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsHL1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]
    # get district id
    district = request.POST.get('dist')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 10 AND 19), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where div_id = "+str(request.POST.get('div'))+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 10 AND 19), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = "+str(district)+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataHL1(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 10 AND 19), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f  where dist_id = "+str(request.POST.get('dist'))+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 10 AND 19), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where userid = "+str(user_id)+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))

def health2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_hh_form_submission,total_hh_member,ratio
    # data stored in division_data

    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 2), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id, division, Count(*) total_women, Count(DISTINCT household_id) total_hh, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio FROM f GROUP BY div_id, division"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)

    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    # total ratio calculation
    total_women = 0
    for each in df.total_women.tolist():
        total_women = total_women + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_women) / float(total_hh))
    else:
        total_ratio = 0

    # for setting division_id and division_name
    query = "select distinct div_id,division from vw_geo_data_catchment "
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # for intial chart data load
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 2), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/health2.html', {'division': division,
                                                          'divison_data': divison_data,
                                                          'total_women': total_women,
                                                          'total_hh': total_hh,
                                                          'total_ratio': total_ratio,
                                                          'categories': categories,
                                                          'data': data
                                                          })


def getDistrictsHL2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 2), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id, division, Count(*) total_women, Count(DISTINCT household_id) total_hh, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio FROM f GROUP BY div_id, division"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_women = 0
        for each in df.total_women.tolist():
            total_women = total_women + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_women) / float(total_hh))
        else:
            total_ratio = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_women': total_women,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 2), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id, district, Count(*) total_women, Count(DISTINCT household_id) total_hh, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio FROM f where div_id = "+str(division)+" GROUP BY dist_id, district"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_women = 0
    for each in df.total_women.tolist():
        total_women = total_women + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_women) / float(total_hh))
    else:
        total_ratio = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'district_data': district_data,
                                    'total_women': total_women,
                                    'total_hh': total_hh,
                                    'total_ratio': total_ratio}))


def getUsersHL2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 2), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id, district, Count(*) total_women, Count(DISTINCT household_id) total_hh, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio FROM f where div_id = "+str(request.POST.get('div'))+" GROUP BY dist_id, district"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_women = 0
        for each in df.total_women.tolist():
            total_women = total_women + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_women) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_women': total_women,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))
    else:
        # query returns user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 2), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), Count(*) total_women, Count(DISTINCT household_id) total_hh, Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) AS ratio FROM f where dist_id = "+str(district)+" GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_women = 0
        for each in df.total_women.tolist():
            total_women = total_women + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_women) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_women': total_women,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))


def getDistrictsChartsHL2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 2), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where div_id = "+str(division)+" GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsHL2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]
    # get district id
    district = request.POST.get('dist')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 2), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where div_id = "+str(request.POST.get('div'))+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 2), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = "+str(district)+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataHL2(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 2), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = "+str(request.POST.get('dist'))+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Last_delivery_month' ) :: INT = 2), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where userid = "+str(user_id)+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))



def hiv_aids(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_hh_form_submission,total_hh_member,ratio
    # data stored in division_data

    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT BETWEEN 15 AND 24 ), f AS (SELECT h.*, h.user_id AS userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id, division, Count(*) total_youth, Count(DISTINCT household_id) total_hh, CASE WHEN Count(DISTINCT household_id) :: FLOAT = 0 THEN 0 ELSE Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC, 2) END AS ratio FROM f GROUP BY div_id, division "
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)

    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    # total ratio calculation
    total_youth = 0
    for each in df.total_youth.tolist():
        total_youth = total_youth + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_youth) / float(total_hh))
    else:
        total_ratio = 0

    # for setting division_id and division_name
    query = "select distinct div_id,division from vw_geo_data_catchment "
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # for intial chart data load
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT between 15 and 24 ), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC,2) end AS ratio FROM f GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/hiv_aids.html', {'division': division,
                                                          'divison_data': divison_data,
                                                          'total_youth': total_youth,
                                                          'total_hh': total_hh,
                                                          'total_ratio': total_ratio,
                                                          'categories': categories,
                                                          'data': data
                                                          })


def getDistrictsHA(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT between 15 and 24 ), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT div_id, division, Count(*) total_youth, Count(DISTINCT household_id) total_hh, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC,2) end AS ratio FROM f GROUP BY div_id, division"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_youth = 0
        for each in df.total_youth.tolist():
            total_youth = total_youth + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_youth) / float(total_hh))
        else:
            total_ratio = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_youth': total_youth,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT between 15 and 24 ), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id, district, Count(*) total_youth, Count(DISTINCT household_id) total_hh, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC,2) end AS ratio FROM f where div_id = "+str(division)+" GROUP BY dist_id, district"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_youth = 0
    for each in df.total_youth.tolist():
        total_youth = total_youth + each
    total_hh = 0
    for each in df.total_hh.tolist():
        total_hh = total_hh + each
    if total_hh != 0:
        total_ratio = (float(total_youth) / float(total_hh))
    else:
        total_ratio = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'district_data': district_data,
                                    'total_youth': total_youth,
                                    'total_hh': total_hh,
                                    'total_ratio': total_ratio}))


def getUsersHA(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT between 15 and 24 ), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT dist_id, district, Count(*) total_youth, Count(DISTINCT household_id) total_hh, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC,2) end AS ratio FROM f where div_id = "+str(request.POST.get('div'))+" GROUP BY dist_id, district"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_youth = 0
        for each in df.total_youth.tolist():
            total_youth = total_youth + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_youth) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_youth': total_youth,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))
    else:
        # query returns user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT between 15 and 24 ), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid, (select username from auth_user where id = userid), Count(*) total_youth, Count(DISTINCT household_id) total_hh, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC,2) end AS ratio FROM f where dist_id = "+str(district)+" GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_youth = 0
        for each in df.total_youth.tolist():
            total_youth = total_youth + each
        total_hh = 0
        for each in df.total_hh.tolist():
            total_hh = total_hh + each
        if total_hh != 0:
            total_ratio = (float(total_youth) / float(total_hh))
        else:
            total_ratio = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_youth': total_youth,
                                        'total_hh': total_hh,
                                        'total_ratio': total_ratio}))


def getDistrictsChartsHA(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT between 15 and 24 ), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC,2) end AS ratio FROM f where div_id = "+str(division)+" GROUP BY date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsHA(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]
    # get district id
    district = request.POST.get('dist')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT between 15 and 24 ), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC,2) end AS ratio FROM f where div_id = "+str(request.POST.get('div'))+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT between 15 and 24 ), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC,2) end AS ratio FROM f where dist_id = "+str(district)+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataHA(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT between 15 and 24 ), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC,2) end AS ratio FROM f where dist_id = "+str(request.POST.get('dist'))+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, Json_array_elements(( json ->> 'group_member') :: json) group_member , ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), h AS (SELECT user_id, date_created, household_id FROM t WHERE ( group_member ->> 'group_member/Age_year' ) :: INT between 15 and 24 ), f AS (SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT date_created, case when Count(DISTINCT household_id) :: FLOAT = 0 then 0 else Round(( Count(*) :: FLOAT / Count(DISTINCT household_id) :: FLOAT ) :: NUMERIC,2) end AS ratio FROM f where userid = "+str(user_id)+" GROUP BY date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['ratio'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))


def child_marriage(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching child_marriage form id
    query = "select id from public.logger_xform where id_string = 'child_marriage'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    cm_form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_household,total_hh_with_member, percentage
    # data stored in division_data

    query = "WITH h AS( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS ( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(cm_form_id)+" AND deleted_at IS NULL AND substring (( json ->> 'HH_member_got_married_last_1year' ) FROM 1 FOR 1 ) != '3') , ebf1 AS ( SELECT ebf.*, vw_geo_data_catchment.* FROM ebf INNER JOIN vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS ( SELECT h.*, vw_geo_data_catchment.* FROM h INNER JOIN vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS ( SELECT division, div_id, count(DISTINCT household_id) total_hh_with_member FROM ebf1 GROUP BY division, div_id) SELECT f.division, f.div_id, count(DISTINCT household_id) total_household, total_hh_with_member, case when count(DISTINCT household_id) :: float = 0 then 0 else round(( total_hh_with_member :: float / count(DISTINCT household_id) :: float ) :: numeric * 100, 2) end AS percentage FROM f, ebf2 WHERE ebf2.div_id = f.div_id GROUP BY f.division, total_hh_with_member, f.div_id"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)

    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    # total percentage calculation
    total_household = 0
    for each in df.total_household.tolist():
        total_household = total_household + each
    total_hh_with_member = 0
    for each in df.total_hh_with_member.tolist():
        total_hh_with_member = total_hh_with_member + each
    if total_household != 0:
        total_percentage = (float(total_hh_with_member) / float(total_household))
    else:
        total_percentage = 0

    # for setting division_id and division_name
    query = "select distinct div_id,division from vw_geo_data_catchment "
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # for intial chart data load
    query = "WITH hf AS( SELECT DISTINCT user_id, date_created ::date AS date_created, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL ), cm AS ( SELECT user_id, date_created :: date AS date_created, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(cm_form_id)+" AND deleted_at IS NULL AND substring (( json ->> 'HH_member_got_married_last_1year' ) FROM 1 FOR 1 ) != '3' ) , hf1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_hh FROM hf GROUP BY user_id, date_created), cm1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_household FROM cm GROUP BY user_id, date_created), hf2 AS ( SELECT hf1.*, vw_geo_data_catchment.* FROM hf1 INNER JOIN PUBLIC.vw_geo_data_catchment ON hf1.user_id = vw_geo_data_catchment.user_id), cm2 AS ( SELECT cm1.*, vw_geo_data_catchment.* FROM cm1 INNER JOIN PUBLIC.vw_geo_data_catchment ON cm1.user_id = vw_geo_data_catchment.user_id) SELECT hf2.date_created, round(( sum(total_household) :: float / sum(total_hh) :: float ) :: numeric * 100, 2) AS percentage FROM cm2, hf2 WHERE cm2.date_created = hf2.date_created GROUP BY hf2.date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/child_marriage.html', {'division': division,
                                                          'divison_data': divison_data,
                                                          'total_household': total_household,
                                                          'total_hh_with_member': total_hh_with_member,
                                                          'total_percentage': total_percentage,
                                                          'categories': categories,
                                                          'data': data
                                                          })


def getDistrictsCM(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching child_marriage form id
    query = "select id from public.logger_xform where id_string = 'child_marriage'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    cm_form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "WITH h AS( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS ( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(cm_form_id)+" AND deleted_at IS NULL AND substring (( json ->> 'HH_member_got_married_last_1year' ) FROM 1 FOR 1 ) != '3') , ebf1 AS ( SELECT ebf.*, vw_geo_data_catchment.* FROM ebf INNER JOIN vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS ( SELECT h.*, vw_geo_data_catchment.* FROM h INNER JOIN vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS ( SELECT division, div_id, count(DISTINCT household_id) total_hh_with_member FROM ebf1 GROUP BY division, div_id) SELECT f.division, f.div_id, count(DISTINCT household_id) total_household, total_hh_with_member, case when count(DISTINCT household_id) :: float = 0 then 0 else round(( total_hh_with_member :: float / count(DISTINCT household_id) :: float ) :: numeric * 100, 2) end AS percentage FROM f, ebf2 WHERE ebf2.div_id = f.div_id GROUP BY f.division, total_hh_with_member, f.div_id"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        total_hh_with_member = 0
        for each in df.total_hh_with_member.tolist():
            total_hh_with_member = total_hh_with_member + each
        if total_household != 0:
            total_percentage = (float(total_household) / float(total_hh_with_member))
        else:
            total_percentage = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_household': total_household,
                                        'total_hh_with_member': total_hh_with_member,
                                        'total_percentage': total_percentage}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "WITH h AS( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS ( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(cm_form_id)+" AND deleted_at IS NULL AND substring (( json ->> 'HH_member_got_married_last_1year' ) FROM 1 FOR 1 ) != '3') , ebf1 AS ( SELECT ebf.*, vw_geo_data_catchment.* FROM ebf INNER JOIN vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS ( SELECT h.*, vw_geo_data_catchment.* FROM h INNER JOIN vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS ( SELECT district, dist_id, count(DISTINCT household_id) total_hh_with_member FROM ebf1 where div_id = "+str(division)+" GROUP BY district, dist_id) SELECT f.district, f.dist_id, count(DISTINCT household_id) total_household, total_hh_with_member, case when count(DISTINCT household_id) :: float = 0 then 0 else round(( total_hh_with_member :: float / count(DISTINCT household_id) :: float ) :: numeric * 100, 2) end AS percentage FROM f, ebf2 WHERE ebf2.dist_id = f.dist_id GROUP BY f.district, f.dist_id, total_hh_with_member"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_household = 0
    for each in df.total_household.tolist():
        total_household = total_household + each
    total_hh_with_member = 0
    for each in df.total_hh_with_member.tolist():
        total_hh_with_member = total_hh_with_member + each
    if total_household != 0:
        total_percentage = (float(total_hh_with_member) / float(total_household))
    else:
        total_percentage = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'district_data': district_data,
                                    'total_household': total_household,
                                    'total_hh_with_member': total_hh_with_member,
                                    'total_percentage': total_percentage}))


def getUsersCM(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching child_marriage form id
    query = "select id from public.logger_xform where id_string = 'child_marriage'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    cm_form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "WITH h AS( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS ( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(cm_form_id)+" AND deleted_at IS NULL AND substring (( json ->> 'HH_member_got_married_last_1year' ) FROM 1 FOR 1 ) != '3') , ebf1 AS ( SELECT ebf.*, vw_geo_data_catchment.* FROM ebf INNER JOIN vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS ( SELECT h.*, vw_geo_data_catchment.* FROM h INNER JOIN vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS ( SELECT district, dist_id, count(DISTINCT household_id) total_hh_with_member FROM ebf1 where div_id = "+str(request.POST.get('div'))+" GROUP BY district, dist_id) SELECT f.district, f.dist_id, count(DISTINCT household_id) total_household, total_hh_with_member, case when count(DISTINCT household_id) :: float = 0 then 0 else round(( total_hh_with_member :: float / count(DISTINCT household_id) :: float ) :: numeric * 100, 2) end AS percentage FROM f, ebf2 WHERE ebf2.dist_id = f.dist_id GROUP BY f.district, f.dist_id, total_hh_with_member"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        total_hh_with_member = 0
        for each in df.total_hh_with_member.tolist():
            total_hh_with_member = total_hh_with_member + each
        if total_household != 0:
            total_percentage = (float(total_hh_with_member ) / float(total_household))
        else:
            total_percentage = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_household': total_household,
                                        'total_hh_with_member': total_hh_with_member,
                                        'total_percentage': total_percentage}))
    else:
        # query returns user wise data
        query = "WITH h AS( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS ( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(cm_form_id)+" AND deleted_at IS NULL AND substring (( json ->> 'HH_member_got_married_last_1year' ) FROM 1 FOR 1 ) != '3') , ebf1 AS ( SELECT ebf.*,ebf.user_id as userid, vw_geo_data_catchment.* FROM ebf INNER JOIN vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS ( SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h INNER JOIN vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS ( SELECT userid, count(DISTINCT household_id) total_hh_with_member FROM ebf1 where dist_id = "+str(district)+" GROUP BY userid) SELECT f.userid,(select username from auth_user where id = f.userid), count(DISTINCT household_id) total_household, total_hh_with_member, case when count(DISTINCT household_id) :: float = 0 then 0 else round(( total_hh_with_member :: float / count(DISTINCT household_id) :: float ) :: numeric * 100, 2) end AS percentage FROM f, ebf2 WHERE ebf2.userid = f.userid GROUP BY f.userid, total_hh_with_member"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        total_hh_with_member = 0
        for each in df.total_hh_with_member.tolist():
            total_hh_with_member = total_hh_with_member + each
        if total_household != 0:
            total_percentage = (float(total_hh_with_member ) / float(total_household))
        else:
            total_percentage = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_household': total_household,
                                        'total_hh_with_member': total_hh_with_member,
                                        'total_percentage': total_percentage}))


def getDistrictsChartsCM(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching child_marriage form id
    query = "select id from public.logger_xform where id_string = 'child_marriage'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    cm_form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH hf AS( SELECT DISTINCT user_id, date_created ::date AS date_created, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL ), cm AS ( SELECT user_id, date_created :: date AS date_created, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(cm_form_id)+" AND deleted_at IS NULL AND substring (( json ->> 'HH_member_got_married_last_1year' ) FROM 1 FOR 1 ) != '3' ) , hf1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_hh FROM hf GROUP BY user_id, date_created), cm1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_household FROM cm GROUP BY user_id, date_created), hf2 AS ( SELECT hf1.*, vw_geo_data_catchment.* FROM hf1 INNER JOIN PUBLIC.vw_geo_data_catchment ON hf1.user_id = vw_geo_data_catchment.user_id), cm2 AS ( SELECT cm1.*, vw_geo_data_catchment.* FROM cm1 INNER JOIN PUBLIC.vw_geo_data_catchment ON cm1.user_id = vw_geo_data_catchment.user_id) SELECT hf2.date_created, round(( sum(total_household) :: float / sum(total_hh) :: float ) :: numeric * 100, 2) AS percentage FROM cm2, hf2 WHERE cm2.date_created = hf2.date_created  and cm2.div_id = "+str(division)+" GROUP BY hf2.date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsCM(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching child_marriage form id
    query = "select id from public.logger_xform where id_string = 'child_marriage'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    cm_form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')


    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH hf AS( SELECT DISTINCT user_id, date_created ::date AS date_created, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL ), cm AS ( SELECT user_id, date_created :: date AS date_created, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(cm_form_id)+" AND deleted_at IS NULL AND substring (( json ->> 'HH_member_got_married_last_1year' ) FROM 1 FOR 1 ) != '3' ) , hf1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_hh FROM hf GROUP BY user_id, date_created), cm1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_household FROM cm GROUP BY user_id, date_created), hf2 AS ( SELECT hf1.*, vw_geo_data_catchment.* FROM hf1 INNER JOIN PUBLIC.vw_geo_data_catchment ON hf1.user_id = vw_geo_data_catchment.user_id), cm2 AS ( SELECT cm1.*, vw_geo_data_catchment.* FROM cm1 INNER JOIN PUBLIC.vw_geo_data_catchment ON cm1.user_id = vw_geo_data_catchment.user_id) SELECT hf2.date_created, round(( sum(total_household) :: float / sum(total_hh) :: float ) :: numeric * 100, 2) AS percentage FROM cm2, hf2 WHERE cm2.date_created = hf2.date_created  and cm2.div_id = "+str(request.POST.get('div'))+" GROUP BY hf2.date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH hf AS( SELECT DISTINCT user_id, date_created ::date AS date_created, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL ), cm AS ( SELECT user_id, date_created :: date AS date_created, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(cm_form_id)+" AND deleted_at IS NULL AND substring (( json ->> 'HH_member_got_married_last_1year' ) FROM 1 FOR 1 ) != '3' ) , hf1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_hh FROM hf GROUP BY user_id, date_created), cm1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_household FROM cm GROUP BY user_id, date_created), hf2 AS ( SELECT hf1.*, vw_geo_data_catchment.* FROM hf1 INNER JOIN PUBLIC.vw_geo_data_catchment ON hf1.user_id = vw_geo_data_catchment.user_id), cm2 AS ( SELECT cm1.*, vw_geo_data_catchment.* FROM cm1 INNER JOIN PUBLIC.vw_geo_data_catchment ON cm1.user_id = vw_geo_data_catchment.user_id) SELECT hf2.date_created, round(( sum(total_household) :: float / sum(total_hh) :: float ) :: numeric * 100, 2) AS percentage FROM cm2, hf2 WHERE cm2.date_created = hf2.date_created  and cm2.dist_id = "+str(district)+" GROUP BY hf2.date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataCM(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching child_marriage form id
    query = "select id from public.logger_xform where id_string = 'child_marriage'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    cm_form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH hf AS( SELECT DISTINCT user_id, date_created ::date AS date_created, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL ), cm AS ( SELECT user_id, date_created :: date AS date_created, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(cm_form_id)+" AND deleted_at IS NULL AND substring (( json ->> 'HH_member_got_married_last_1year' ) FROM 1 FOR 1 ) != '3' ) , hf1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_hh FROM hf GROUP BY user_id, date_created), cm1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_household FROM cm GROUP BY user_id, date_created), hf2 AS ( SELECT hf1.*, vw_geo_data_catchment.* FROM hf1 INNER JOIN PUBLIC.vw_geo_data_catchment ON hf1.user_id = vw_geo_data_catchment.user_id), cm2 AS ( SELECT cm1.*, vw_geo_data_catchment.* FROM cm1 INNER JOIN PUBLIC.vw_geo_data_catchment ON cm1.user_id = vw_geo_data_catchment.user_id) SELECT hf2.date_created, round(( sum(total_household) :: float / sum(total_hh) :: float ) :: numeric * 100, 2) AS percentage FROM cm2, hf2 WHERE cm2.date_created = hf2.date_created  and cm2.dist_id = "+str(request.POST.get('dist'))+" GROUP BY hf2.date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = " WITH hf AS( SELECT DISTINCT user_id, date_created ::date AS date_created, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL ), cm AS ( SELECT user_id, date_created :: date AS date_created, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(cm_form_id)+" AND deleted_at IS NULL AND substring (( json ->> 'HH_member_got_married_last_1year' ) FROM 1 FOR 1 ) != '3' ) , hf1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_hh FROM hf GROUP BY user_id, date_created), cm1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_household FROM cm GROUP BY user_id, date_created), hf2 AS ( SELECT hf1.*, vw_geo_data_catchment.* FROM hf1 INNER JOIN PUBLIC.vw_geo_data_catchment ON hf1.user_id = vw_geo_data_catchment.user_id), cm2 AS ( SELECT cm1.*,cm1.user_id as userid, vw_geo_data_catchment.* FROM cm1 INNER JOIN PUBLIC.vw_geo_data_catchment ON cm1.user_id = vw_geo_data_catchment.user_id) SELECT hf2.date_created, round(( sum(total_household) :: float / sum(total_hh) :: float ) :: numeric * 100, 2) AS percentage FROM cm2, hf2 WHERE cm2.date_created = hf2.date_created and cm2.userid = "+str(user_id)+" GROUP BY hf2.date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))





def wash(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching sanitation form id
    query = "select id from public.logger_xform where id_string = 'sanitation'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    s_form_id = df.id.tolist()[0]

    # query for division data
    # returns division,total_household,total_hh_with_member, percentage
    # data stored in division_data

    query = "WITH h AS( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS ( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(s_form_id)+" AND deleted_at IS NULL AND ( json ->> 'latrine_type' )::int between 21 and 24 ) , ebf1 AS ( SELECT ebf.*, vw_geo_data_catchment.* FROM ebf INNER JOIN vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS ( SELECT h.*, vw_geo_data_catchment.* FROM h INNER JOIN vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS ( SELECT division, div_id, count(DISTINCT household_id) total_hh_with_member FROM ebf1 GROUP BY division, div_id) SELECT f.division, f.div_id, count(DISTINCT household_id) total_household, total_hh_with_member, CASE WHEN count(DISTINCT household_id) :: float = 0 THEN 0 ELSE round(( total_hh_with_member :: float / count(DISTINCT household_id) :: float ) :: numeric * 100, 2) END AS percentage FROM f, ebf2 WHERE ebf2.div_id = f.div_id GROUP BY f.division, total_hh_with_member, f.div_id"
    divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)

    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    # total percentage calculation
    total_household = 0
    for each in df.total_household.tolist():
        total_household = total_household + each
    total_hh_with_member = 0
    for each in df.total_hh_with_member.tolist():
        total_hh_with_member = total_hh_with_member + each
    if total_household != 0:
        total_percentage = (float(total_hh_with_member) / float(total_household))
    else:
        total_percentage = 0

    # for setting division_id and division_name
    query = "select distinct div_id,division from vw_geo_data_catchment "
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    div_value = df.div_id.tolist()
    div_name = df.division.tolist()
    division = zip(div_value, div_name)

    # for intial chart data load
    query = "WITH hf AS( SELECT DISTINCT user_id, date_created ::date AS date_created, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL ), cm AS ( SELECT user_id, date_created :: date AS date_created, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(s_form_id)+" AND deleted_at IS NULL AND ( json ->> 'latrine_type' )::int between 21 and 24 ) , hf1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_hh FROM hf GROUP BY user_id, date_created), cm1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_household FROM cm GROUP BY user_id, date_created), hf2 AS ( SELECT hf1.*, vw_geo_data_catchment.* FROM hf1 INNER JOIN PUBLIC.vw_geo_data_catchment ON hf1.user_id = vw_geo_data_catchment.user_id), cm2 AS ( SELECT cm1.*, vw_geo_data_catchment.* FROM cm1 INNER JOIN PUBLIC.vw_geo_data_catchment ON cm1.user_id = vw_geo_data_catchment.user_id) SELECT hf2.date_created, round(( sum(total_household) :: float / sum(total_hh) :: float ) :: numeric * 100, 2) AS percentage FROM cm2, hf2 WHERE cm2.date_created = hf2.date_created GROUP BY hf2.date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
    return render(request, 'hhp_module/wash.html', {'division': division,
                                                          'divison_data': divison_data,
                                                          'total_household': total_household,
                                                          'total_hh_with_member': total_hh_with_member,
                                                          'total_percentage': total_percentage,
                                                          'categories': categories,
                                                          'data': data
                                                          })


def getDistrictsWH(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching sanitation form id
    query = "select id from public.logger_xform where id_string = 'sanitation'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    s_form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')

    if math.isnan(float(division)):
        # query returns all data
        query = "WITH h AS( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS ( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(s_form_id)+" AND deleted_at IS NULL AND ( json ->> 'latrine_type' )::int between 21 and 24 ) , ebf1 AS ( SELECT ebf.*, vw_geo_data_catchment.* FROM ebf INNER JOIN vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS ( SELECT h.*, vw_geo_data_catchment.* FROM h INNER JOIN vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS ( SELECT division, div_id, count(DISTINCT household_id) total_hh_with_member FROM ebf1 GROUP BY division, div_id) SELECT f.division, f.div_id, count(DISTINCT household_id) total_household, total_hh_with_member, CASE WHEN count(DISTINCT household_id) :: float = 0 THEN 0 ELSE round(( total_hh_with_member :: float / count(DISTINCT household_id) :: float ) :: numeric * 100, 2) END AS percentage FROM f, ebf2 WHERE ebf2.div_id = f.div_id GROUP BY f.division, total_hh_with_member, f.div_id"
        divison_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)

        # total ratio calculation
        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        total_hh_with_member = 0
        for each in df.total_hh_with_member.tolist():
            total_hh_with_member = total_hh_with_member + each
        if total_household != 0:
            total_percentage = (float(total_household) / float(total_hh_with_member))
        else:
            total_percentage = 0

        return HttpResponse(json.dumps({'divison_data': divison_data,
                                        'total_household': total_household,
                                        'total_hh_with_member': total_hh_with_member,
                                        'total_percentage': total_percentage}))

    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for fetching data for that division id
    query = "WITH h AS( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS ( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(s_form_id)+" AND deleted_at IS NULL AND ( json ->> 'latrine_type' )::int between 21 and 24 ) , ebf1 AS ( SELECT ebf.*, vw_geo_data_catchment.* FROM ebf INNER JOIN vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS ( SELECT h.*, vw_geo_data_catchment.* FROM h INNER JOIN vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS ( SELECT district, dist_id, count(DISTINCT household_id) total_hh_with_member FROM ebf1 where div_id = "+str(division)+" GROUP BY district, dist_id) SELECT f.district, f.dist_id, count(DISTINCT household_id) total_household, total_hh_with_member, CASE WHEN count(DISTINCT household_id) :: float = 0 THEN 0 ELSE round(( total_hh_with_member :: float / count(DISTINCT household_id) :: float ) :: numeric * 100, 2) END AS percentage FROM f, ebf2 WHERE ebf2.dist_id = f.dist_id GROUP BY f.district, total_hh_with_member, f.dist_id"
    district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)

    # total ratio calculation
    total_household = 0
    for each in df.total_household.tolist():
        total_household = total_household + each
    total_hh_with_member = 0
    for each in df.total_hh_with_member.tolist():
        total_hh_with_member = total_hh_with_member + each
    if total_household != 0:
        total_percentage = (float(total_hh_with_member) / float(total_household))
    else:
        total_percentage = 0
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'district_data': district_data,
                                    'total_household': total_household,
                                    'total_hh_with_member': total_hh_with_member,
                                    'total_percentage': total_percentage}))


def getUsersWH(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching sanitation form id
    query = "select id from public.logger_xform where id_string = 'sanitation'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    s_form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')

    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query returns division wise data
        query = "WITH h AS( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), ebf AS ( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM PUBLIC.logger_instance WHERE xform_id = " + str(
            s_form_id) + " AND deleted_at IS NULL AND ( json ->> 'latrine_type' )::int between 21 and 24 ) , ebf1 AS ( SELECT ebf.*, vw_geo_data_catchment.* FROM ebf INNER JOIN vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS ( SELECT h.*, vw_geo_data_catchment.* FROM h INNER JOIN vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS ( SELECT district, dist_id, count(DISTINCT household_id) total_hh_with_member FROM ebf1 where div_id = " + str(
            request.POST.get('div')) + " GROUP BY district, dist_id) SELECT f.district, f.dist_id, count(DISTINCT household_id) total_household, total_hh_with_member, CASE WHEN count(DISTINCT household_id) :: float = 0 THEN 0 ELSE round(( total_hh_with_member :: float / count(DISTINCT household_id) :: float ) :: numeric * 100, 2) END AS percentage FROM f, ebf2 WHERE ebf2.dist_id = f.dist_id GROUP BY f.district, total_hh_with_member, f.dist_id"
        district_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        total_hh_with_member = 0
        for each in df.total_hh_with_member.tolist():
            total_hh_with_member = total_hh_with_member + each
        if total_household != 0:
            total_percentage = (float(total_hh_with_member ) / float(total_household))
        else:
            total_percentage = 0
        return HttpResponse(json.dumps({'district_data': district_data,
                                        'total_household': total_household,
                                        'total_hh_with_member': total_hh_with_member,
                                        'total_percentage': total_percentage}))
    else:
        # query returns user wise data
        query = "WITH h AS( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL), ebf AS ( SELECT id, user_id, date_created :: date, ( json ->> 'hh_id') household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(s_form_id)+" AND deleted_at IS NULL AND ( json ->> 'latrine_type' )::int between 21 and 24 ) , ebf1 AS ( SELECT ebf.*,ebf.user_id as userid, vw_geo_data_catchment.* FROM ebf INNER JOIN vw_geo_data_catchment ON ebf.user_id = vw_geo_data_catchment.user_id), f AS ( SELECT h.*,h.user_id as userid, vw_geo_data_catchment.* FROM h INNER JOIN vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id), ebf2 AS ( SELECT userid, count(DISTINCT household_id) total_hh_with_member FROM ebf1 where dist_id = "+str(district)+" GROUP BY userid) SELECT f.userid,(select username from auth_user where id = f.userid), count(DISTINCT household_id) total_household, total_hh_with_member, CASE WHEN count(DISTINCT household_id) :: float = 0 THEN 0 ELSE round(( total_hh_with_member :: float / count(DISTINCT household_id) :: float ) :: numeric * 100, 2) END AS percentage FROM f, ebf2 WHERE ebf2.userid = f.userid GROUP BY f.userid, total_hh_with_member "
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        # total ratio calculation
        total_household = 0
        for each in df.total_household.tolist():
            total_household = total_household + each
        total_hh_with_member = 0
        for each in df.total_hh_with_member.tolist():
            total_hh_with_member = total_hh_with_member + each
        if total_household != 0:
            total_percentage = (float(total_hh_with_member ) / float(total_household))
        else:
            total_percentage = 0
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'total_household': total_household,
                                        'total_hh_with_member': total_hh_with_member,
                                        'total_percentage': total_percentage}))


def getDistrictsChartsWH(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching sanitation form id
    query = "select id from public.logger_xform where id_string = 'sanitation'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    s_form_id = df.id.tolist()[0]

    # get division id
    division = request.POST.get('div')
    district_query = "select DISTINCT dist_id,district from vw_geo_data_catchment where div_id = " + str(division)
    district_id_value = json.dumps(__db_fetch_values_dict(district_query))

    # query for categories and data in a division
    query = "WITH hf AS( SELECT DISTINCT user_id, date_created ::date AS date_created, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL ), cm AS ( SELECT user_id, date_created :: date AS date_created, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(s_form_id)+" AND deleted_at IS NULL AND ( json ->> 'latrine_type' )::int between 21 and 24 ) , hf1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_hh FROM hf GROUP BY user_id, date_created), cm1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_household FROM cm GROUP BY user_id, date_created), hf2 AS ( SELECT hf1.*, vw_geo_data_catchment.* FROM hf1 INNER JOIN PUBLIC.vw_geo_data_catchment ON hf1.user_id = vw_geo_data_catchment.user_id), cm2 AS ( SELECT cm1.*, vw_geo_data_catchment.* FROM cm1 INNER JOIN PUBLIC.vw_geo_data_catchment ON cm1.user_id = vw_geo_data_catchment.user_id) SELECT hf2.date_created, round(( sum(total_household) :: float / sum(total_hh) :: float ) :: numeric * 100, 2) AS percentage FROM cm2, hf2 WHERE cm2.date_created = hf2.date_created and cm2.div_id = "+str(division)+" GROUP BY hf2.date_created"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
    data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
    return HttpResponse(json.dumps({'district_id_value': district_id_value,
                                    'categories': categories,
                                    'data': data}))


def getUsersChartsWH(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching sanitation form id
    query = "select id from public.logger_xform where id_string = 'sanitation'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    s_form_id = df.id.tolist()[0]

    # get district id
    district = request.POST.get('dist')


    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(district)):
        # query for all data
        query = "WITH hf AS( SELECT DISTINCT user_id, date_created ::date AS date_created, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL ), cm AS ( SELECT user_id, date_created :: date AS date_created, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(s_form_id)+" AND deleted_at IS NULL AND ( json ->> 'latrine_type' )::int between 21 and 24 ) , hf1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_hh FROM hf GROUP BY user_id, date_created), cm1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_household FROM cm GROUP BY user_id, date_created), hf2 AS ( SELECT hf1.*, vw_geo_data_catchment.* FROM hf1 INNER JOIN PUBLIC.vw_geo_data_catchment ON hf1.user_id = vw_geo_data_catchment.user_id), cm2 AS ( SELECT cm1.*, vw_geo_data_catchment.* FROM cm1 INNER JOIN PUBLIC.vw_geo_data_catchment ON cm1.user_id = vw_geo_data_catchment.user_id) SELECT hf2.date_created, round(( sum(total_household) :: float / sum(total_hh) :: float ) :: numeric * 100, 2) AS percentage FROM cm2, hf2 WHERE cm2.date_created = hf2.date_created and cm2.div_id = "+str(request.POST.get('div'))+" GROUP BY hf2.date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for username and user id
        query = "WITH t AS(SELECT id, user_id, date_created :: DATE, ( json ->> 'No_of_HH_member') :: INT hh_member, ( json ->> 'hh_id' ) household_id FROM logger_instance WHERE xform_id = " + str(
            form_id) + " AND deleted_at IS NULL), h AS (SELECT user_id, date_created, hh_member, household_id FROM t), f AS (SELECT h.*, h.user_id as userid, vw_geo_data_catchment.* FROM h inner join vw_geo_data_catchment ON h.user_id = vw_geo_data_catchment.user_id) SELECT userid,(select username from auth_user where id = userid), case when SUM(hh_member) = 0 then 0 else Round(( Count(distinct household_id) :: FLOAT / SUM(hh_member) :: FLOAT ) :: NUMERIC, 2) end AS ratio FROM f where dist_id = " + str(
            district) + " GROUP BY userid"
        user_data = json.dumps(__db_fetch_values_dict(query), default=decimal_date_default)
        # query for district wise data
        query = "WITH hf AS( SELECT DISTINCT user_id, date_created ::date AS date_created, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL ), cm AS ( SELECT user_id, date_created :: date AS date_created, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(s_form_id)+" AND deleted_at IS NULL AND ( json ->> 'latrine_type' )::int between 21 and 24 ) , hf1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_hh FROM hf GROUP BY user_id, date_created), cm1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_household FROM cm GROUP BY user_id, date_created), hf2 AS ( SELECT hf1.*, vw_geo_data_catchment.* FROM hf1 INNER JOIN PUBLIC.vw_geo_data_catchment ON hf1.user_id = vw_geo_data_catchment.user_id), cm2 AS ( SELECT cm1.*, vw_geo_data_catchment.* FROM cm1 INNER JOIN PUBLIC.vw_geo_data_catchment ON cm1.user_id = vw_geo_data_catchment.user_id) SELECT hf2.date_created, round(( sum(total_household) :: float / sum(total_hh) :: float ) :: numeric * 100, 2) AS percentage FROM cm2, hf2 WHERE cm2.date_created = hf2.date_created and cm2.dist_id = "+str(district)+" GROUP BY hf2.date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'user_data': user_data,
                                        'categories': categories,
                                        'data': data}))


def getIndividualUsersDataWH(request):
    # query for fetching hh_information form id
    query = "select id from logger_xform where id_string = 'hh_information'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    form_id = df.id.tolist()[0]

    # query for fetching sanitation form id
    query = "select id from public.logger_xform where id_string = 'sanitation'"
    df = pandas.DataFrame()
    df = pandas.read_sql(query, connection)
    s_form_id = df.id.tolist()[0]

    # get user id
    user_id = request.POST.get('user_id')
    # check if it is a NaN or not
    # other way if it is selected 'ALL' in select element from front end html or not
    if math.isnan(float(user_id)):
        # query for district wise data
        query = "WITH hf AS( SELECT DISTINCT user_id, date_created ::date AS date_created, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL ), cm AS ( SELECT user_id, date_created :: date AS date_created, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(s_form_id)+" AND deleted_at IS NULL AND ( json ->> 'latrine_type' )::int between 21 and 24 ) , hf1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_hh FROM hf GROUP BY user_id, date_created), cm1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_household FROM cm GROUP BY user_id, date_created), hf2 AS ( SELECT hf1.*, vw_geo_data_catchment.* FROM hf1 INNER JOIN PUBLIC.vw_geo_data_catchment ON hf1.user_id = vw_geo_data_catchment.user_id), cm2 AS ( SELECT cm1.*, vw_geo_data_catchment.* FROM cm1 INNER JOIN PUBLIC.vw_geo_data_catchment ON cm1.user_id = vw_geo_data_catchment.user_id) SELECT hf2.date_created, round(( sum(total_household) :: float / sum(total_hh) :: float ) :: numeric * 100, 2) AS percentage FROM cm2, hf2 WHERE cm2.date_created = hf2.date_created and cm2.dist_id = "+str(request.POST.get('dist'))+" GROUP BY hf2.date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))
    else:
        # query for user wise data
        query = "WITH hf AS( SELECT DISTINCT user_id, date_created ::date AS date_created, ( json ->> 'hh_id') household_id FROM logger_instance WHERE xform_id = "+str(form_id)+" AND deleted_at IS NULL ), cm AS ( SELECT user_id, date_created :: date AS date_created, ( json ->> 'hh_id' ) household_id FROM PUBLIC.logger_instance WHERE xform_id = "+str(s_form_id)+" AND deleted_at IS NULL AND ( json ->> 'latrine_type' )::int between 21 and 24 ) , hf1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_hh FROM hf GROUP BY user_id, date_created), cm1 AS ( SELECT date_created, user_id, count(DISTINCT household_id) AS total_household FROM cm GROUP BY user_id, date_created), hf2 AS ( SELECT hf1.*, vw_geo_data_catchment.* FROM hf1 INNER JOIN PUBLIC.vw_geo_data_catchment ON hf1.user_id = vw_geo_data_catchment.user_id), cm2 AS ( SELECT cm1.*,cm1.user_id as userid, vw_geo_data_catchment.* FROM cm1 INNER JOIN PUBLIC.vw_geo_data_catchment ON cm1.user_id = vw_geo_data_catchment.user_id) SELECT hf2.date_created, round(( sum(total_household) :: float / sum(total_hh) :: float ) :: numeric * 100, 2) AS percentage FROM cm2, hf2 WHERE cm2.date_created = hf2.date_created and cm2.userid = "+str(user_id)+" GROUP BY hf2.date_created"
        df = pandas.DataFrame()
        df = pandas.read_sql(query, connection)
        categories = json.dumps(df['date_created'].tolist(), default=decimal_date_default)
        data = json.dumps(df['percentage'].tolist(), default=decimal_date_default)
        return HttpResponse(json.dumps({'categories': categories,
                                        'data': data}))


def home_page_csvp(request):
    legends_query = "select * from csvp_homepage"
    legends_data = json.dumps(__db_fetch_values(legends_query))
    return render(request,'hhp_module/home_page_csvp.html',{'legends_data' : legends_data})


import re
import shutil
# uid = pwd.getpwnam("root").pw_uid
        # gid = grp.getgrnam("nogroup").gr_gid
        # os.chown('/home/ftpuserifc/'+str(directory),uid,-1)
        # os.chmod('/home/ftpuserifc/'+str(directory), 0o777)
        # print(os.getenv('SUDO_GID'))
        # os.chown('/home/ftpuserifc/'+str(directory), int(os.getenv('SUDO_UID')), int(os.getenv('SUDO_GID')))
        # os.system("sudo chmod -R 777 /home/ftpuserifc/")
        # shutil.copymode('onadata/media/weather_files/', '/home/ftpuserifc/'+str(directory))

def weather_forecast(request):
    start = datetime.now()
    query = "select * from weather_forecast"
    # directory = "20180910_00"
    directory = str(datetime.now().date()).replace('-','')+'_00'
    if not os.path.exists("onadata/media/weather_files/"):
        os.makedirs("onadata/media/weather_files/")
    if not os.path.exists("onadata/media/weather_files/"+str(directory)) and os.path.exists('/home/jubair/weather_files/'+str(directory)):
        shutil.copytree('/home/jubair/weather_files/'+str(directory),'onadata/media/weather_files/'+str(directory))
        #list_of_files in that directory
        list_of_files = os.listdir('onadata/media/weather_files/'+str(directory))
        # print(list_of_files)
        for each in list_of_files:
            print('.txt' in each)
            if '.txt' in each:
                # read from directory
                # shutil.copyfile('onadata/media/test/Tar_02.txt', 'onadata/media/uploaded_files/Tar_02.txt')
                file = open('onadata/media/weather_files/'+str(directory)+'/'+str(each), 'r')
                insert_content = file.read()
                file.close()
                insert_content = insert_content.split('\n')
                for each in insert_content:
                    # print(re.split(r"\s+",each,maxsplit=6))
                    temp_data = each.split(None,6)
                    place_name = ''
                    date_time = ''
                    temperature = ''
                    humidity = ''
                    wind_speed = ''
                    wind_direction = ''
                    rainfall = ''

                    if len(temp_data):
                        place_name = temp_data[0]
                        date_time = str(temp_data[1]).split(':')
                        temperature = temp_data[2]
                        humidity = temp_data[3]
                        wind_speed = temp_data[4]
                        wind_direction = temp_data[5]
                        rainfall = temp_data[6].strip()
                        if rainfall[0] == 'E':
                            rainfall = '-1'

                        # formatting the date_time
                        res_date = ''
                        res_date = date_time[0]+'-'
                        if int(date_time[1]) <= 9:
                            res_date += '0'+date_time[1]+'-'
                        else:
                            res_date += date_time[1] + '-'
                        if int(date_time[2]) <= 9:
                            res_date += '0'+date_time[2]
                        else:
                            res_date += date_time[2]
                        if int(date_time[3]) <= 9:
                            res_date += ' 0' + date_time[3]+ ':00'+ ':00'
                        else:
                            res_date += ' ' + date_time[3] + ':00' + ':00'

                        # check if data is exists or not
                        query = "select id from weather_forecast where place_name = '"+str(place_name)+"' and date_time = '"+str(res_date)+"'"
                        df = pandas.DataFrame()
                        df = pandas.read_sql(query,connection)
                        if not df.empty:
                            # update query
                            id = df.id.tolist()[0]
                            update_query = "UPDATE public.weather_forecast SET temperature='"+str(temperature)+"', humidity='"+str(humidity)+"', wind_speed='"+str(wind_speed)+"', wind_direction='"+str(wind_direction)+"', rainfall='"+str(rainfall)+"' where id = "+str(id)
                            __db_commit_query(update_query)
                        else:
                            # insert query
                            insert_query = "INSERT INTO public.weather_forecast (place_name, date_time, temperature, humidity, wind_speed, wind_direction, rainfall,data_type) VALUES('"+str(place_name)+"', '"+str(res_date)+"', '"+str(temperature)+"', '"+str(humidity)+"', '"+str(wind_speed)+"', '"+str(wind_direction)+"', '"+str(rainfall)+"','forecast')"
                            __db_commit_query(insert_query)
    print(datetime.now()-start)
    return render(request,'hhp_module/index.html')
    # return HttpResponse('')