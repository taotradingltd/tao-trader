import datetime
import os
import re
import tempfile
import time
import threading

import pandas as pd

from django.conf import settings
from django.db import connections
from django.http import FileResponse, HttpResponseBadRequest, JsonResponse
from django.shortcuts import render, redirect
from django.urls import reverse
from django.views.generic import FormView

from gfm_utils.file.clean import clean_dfs
from gfm_utils.helpers import badges, dataframe
from gfm_utils.text import preprocessing, stopwords, utils
from gfm_utils.text.classification import location, taxonomy

from numpy import NaN
from unidecode import unidecode

from data import views, fields, models

from . import forms

MAKING_REPORTS = []

# Class-based views

class MoveReportRequest(FormView):
    form_class = forms.PeopleMoveReportGenerator
    template_name = 'data_science/move_report.html'
    success_url = '/data/people/'

    def post(self, request, *args, **kwargs):
        if 'cancel' in request.POST:
            return redirect(self.success_url)
        else:
            month = request.POST['month']
            year = request.POST['year']
            return redirect(f'/data_science/moves/{year}/{month}/')

class Badges(FormView):
    form_class = forms.BadgesForm
    template_name= 'data_science/badges.html'
    success_url = '/data_science/badges/'

    def post(self, request, *args, **kwargs):
        if 'cancel' in request.POST:
            return redirect(reverse('index'))
        else:
            # do badges stuff
            try:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")

                file = request.FILES.getlist("file")[0]
                dfs = pd.read_excel(file, sheet_name=None, dtype=str)
                dfs = clean_dfs(dfs)

                roundtables = int(request.POST["roundtables"])
                sessions = int(request.POST["sessions"])
                blanks = int(request.POST["blanks"])
                seed = int(request.POST["seed"])
                deviation = int(request.POST["deviation"])
                iterations = int(request.POST["iterations"])

                badges.badge_creation(dfs, roundtables, sessions, blanks,
                    seed, deviation, iterations, tmp.name)

                response = FileResponse(open(tmp.name, 'rb'))
                return response
            finally:
                os.remove(tmp.name)

# Method-based views

def index(request):
    return render(request, 'index.html')

def move_report(request):
    return render(request, 'data_science/move_report.html')

def people_moves(request, year, month):
    global MAKING_REPORTS

    if len(MAKING_REPORTS) >= 5:
        return JsonResponse({'MESSAGE': 'Too many processes running. Try again later.'}, status=503)

    name = settings.MEDIA_ROOT / f'move_reports/{datetime.datetime.strptime(str(month), "%m").strftime("%b")}-{year}-moves.xlsx'

    if os.path.exists(name):
        if time.time() - os.path.getmtime(name) > (10 * 24 * 60 * 60):
            os.remove(name)
            if name not in MAKING_REPORTS:
                t = threading.Thread(target=_people_moves, args=[year, month, name])
                t.setDaemon(True)
                t.start()
                return JsonResponse({'MESSAGE': 'Building report - check back later!'}, status=202)
            else:
                return JsonResponse({'MESSAGE': 'This report is already building - check back later!'}, status=409)
    else:
        if name not in MAKING_REPORTS:
            t = threading.Thread(target=_people_moves, args=[year, month, name])
            t.setDaemon(True)
            t.start()
            return JsonResponse({'MESSAGE': 'Building report - check back later!'}, status=202)
        else:
            return JsonResponse({'MESSAGE': 'This report is already building - check back later!'}, status=409)

    return FileResponse(open(name, 'rb'), status=200)

def _people_moves(year, month, name=''):
    global MAKING_REPORTS

    try:
        MAKING_REPORTS.append(name)
        year = int(year)
        month = int(month)

        target_date = f'{year}-{month}-01'

        # Get full career of everyone who is currently at a hedge fund (or who has just left a hedge fund)
        with connections['default'].cursor() as cursor:
            cursor.execute("""
            SELECT contacts.*, career.location, career.source, company.raum, company.aum_bracket, company.specialism
            FROM contacts
            INNER JOIN career ON career.id = contacts.career_id
            INNER JOIN company ON career.company_id=company.id
            WHERE career.source=%s AND contacts.person_id IN (
                SELECT contacts.person_id
                FROM contacts
                INNER JOIN company_firmtype ON contacts.company_id = company_firmtype.company_id
                WHERE company_firmtype.firmtype_id = 1 AND
                (contacts.date_started=%s OR contacts.date_ended=%s)
            )""", ("Linkedin", target_date, target_date))
            columns = [i[0] for i in cursor.description]

            df = cursor.fetchall()
            df = pd.DataFrame(df, columns=columns)

            df.replace({'': NaN}, inplace=True)

            if not df.shape[0]:
                settings.LOGGER.error("No moves for this month.")
                return

            cursor.execute(f"""
                SELECT company_id, firmtype.value AS firm_type
                FROM company_firmtype
                INNER JOIN firmtype ON company_firmtype.firmtype_id=firmtype.id
                WHERE company_id IN {tuple(set(df['company_id'].to_list()))}
                AND firmtype.subtype=0;
            """)
            columns = [i[0] for i in cursor.description]

            firm_types = cursor.fetchall()
            firm_types = pd.DataFrame(firm_types, columns=columns)

            if not os.getenv("DOCKER"):
                cursor.execute(f"""
                    SELECT *
                    FROM company_format
                    WHERE company_id IN {tuple(set(df['company_id'].to_list()))};
                """)
                columns = [i[0] for i in cursor.description]

                formats = cursor.fetchall()
                formats = pd.DataFrame(formats, columns=columns)

            cursor.execute(f"""
                SELECT person_id, value AS email_address, confidence
                FROM person_email
                WHERE person_id IN {tuple(set(df['person_id'].to_list()))};
            """)
            columns = [i[0] for i in cursor.description]

            emails = cursor.fetchall()
            emails = pd.DataFrame(emails, columns=columns)

            cursor.execute(f"""
                SELECT company.*, GROUP_CONCAT(
                    DISTINCT company_linkedin.value
                    ORDER BY company_linkedin.value
                    SEPARATOR ","
                ) AS linkedin_urls,
                GROUP_CONCAT(
                    DISTINCT company_website.value
                    ORDER BY company_website.value
                    SEPARATOR ","
                ) AS websites,
                GROUP_CONCAT(
                    DISTINCT firmtype.value
                    ORDER BY firmtype.value
                    SEPARATOR "||"
                ) AS firm_types
                FROM company
                LEFT JOIN company_linkedin ON company.id=company_linkedin.company_id
                LEFT JOIN company_website ON company.id=company_website.company_id
                INNER JOIN company_firmtype ON company.id=company_firmtype.company_id
                INNER JOIN firmtype ON firmtype.id=company_firmtype.firmtype_id
                WHERE NOT firmtype.subtype
                GROUP BY company.id;
            """)
            columns = [i[0] for i in cursor.description]

            companies_sheet = cursor.fetchall()
            companies_sheet = pd.DataFrame(companies_sheet, columns=columns)
            companies_sheet = companies_sheet[companies_sheet["firm_types"].str.contains("HF Manager")].drop_duplicates("id")

        df = pd.merge(
            df,
            firm_types,
            'left',
            on='company_id'
        )
        firm_types = df.groupby('career_id')['firm_type'].apply(list).reset_index(name='firm_types')

        df = pd.merge(
            df,
            firm_types,
            'left',
            on='career_id'
        ).drop_duplicates('career_id')

        df['firm_types'] = df['firm_types'].apply(lambda x: '||'.join([i for i in x if type(i) is str]))

        if not os.getenv("DOCKER"):
            df = pd.merge(
                df,
                formats,
                'left',
                on='company_id',
            )
        else:
            df['email_format'] = ""
            df['confidence'] = ""

        df["lastname"] = df["lastname"].fillna("")
        df['fixed_lastname'] = df[['firstname', 'lastname', 'linkedin_profile']].fillna("").apply(lambda x: utils.infer_last_name(x["firstname"], x["lastname"], x["linkedin_profile"]), axis=1)

        # Update fixed last names in the DB
        for idx, row in df.iterrows():
            if re.match("[A-Za-z]{1}\.", row["lastname"]):
                person = models.Person.objects.get(pk=row["person_id"])
                if row["fixed_lastname"]:
                    df.loc[idx, "lastname"] = row["fixed_lastname"]
                    person.lastname = row["fixed_lastname"]
                    person.save()

        df = df.drop(columns="fixed_lastname")

        def process_name_for_email(name):
            name = unidecode(name)

            chars_to_remove = "',. "

            for c in chars_to_remove:
                name = name.replace(c, '')

            return name

        def format_from_row(x):
            x.fillna('', inplace=True)
            x = x.to_dict()

            if x['email_format']:
                if x['firstname'] and x['lastname'] and x['domain']:
                    func = fields.EMAIL_FORMAT_LAMBDA_MAP[x['email_format']]
                    try:
                        result = func(process_name_for_email(x['firstname']), process_name_for_email(x['lastname']), x['domain'])
                        return result if result[0] != "@" else None
                    except:
                        return None
            return None

        df['email_address'] = df.apply(format_from_row, axis=1)
        df = pd.merge(
            df,
            emails,
            'left',
            'person_id'
        )

        df['email_address'] = df['email_address_x']
        df['email_address'].fillna(df['email_address_y'], inplace=True)
        df.drop(columns=['email_address_x', 'email_address_y'], inplace=True)

        df['confidence'] = df['confidence_x']
        df['confidence'].fillna(df['confidence_y'], inplace=True)
        df.drop(columns=['confidence_x', 'confidence_y'], inplace=True)

        not_dones = df[(df['email_address'].isna()) | (df['email_address']=='')]
        not_dones.drop(columns=['email_address', 'confidence'], inplace=True)

        # Format dates
        for date_col in ['date_started', 'date_ended']:
            df[date_col] = pd.to_datetime(df[date_col], format='%Y-%m-%d', errors='coerce')

        # Drop any invalid dates, if present
        df.dropna(subset=['date_started', 'date_ended'], inplace=True)

        # Add in categories
        df['job_level'] = df['role'].fillna('').apply(taxonomy.get_job_level)
        df['job_category'] = df['role'].fillna('').apply(taxonomy.get_job_category)
        df['aum_bracket'] = df['aum_bracket'].fillna(0).apply(lambda x: fields.AUM_BRACKET[int(x)][1])
        df['aum_bracket'] = df['aum_bracket'].apply(lambda x: '' if x == '—' else x)

        # Clean up URLs
        df['linkedin_profile'] = (
            df['linkedin_profile'].str.lower().str.split('/').str[-1].str.split('?').str[0]
        )

        # Get starts for a given month
        starts = df[
            (df['date_started'].dt.year == year)
            & (df['date_started'].dt.month == month)
            & (df['date_ended'] == '1970-01-01')
        ].sort_values(['company_name', 'job_level', 'role'])
        starts.reset_index(drop=True, inplace=True)

        # Get previous roles
        prev_cols = [
            'role',
            'company_id',
            'company_name',
            'raum',
            'aum_bracket',
            'date_started',
            'date_ended',
            'location',
            'job_level',
            'job_category',
            'firm_types',
            'specialism',
        ]

        # List to store previous roles
        previous_roles = []

        for _, row in starts.iterrows():
            try:
                previous_role = (
                    df[
                        (df['person_id'] == row['person_id'])   # get all roles for that person
                        & (df['date_ended'] != '1970-01-01')    # not currently in that role
                    ]
                    .sort_values('date_ended', ascending=False)
                    .iloc[0]                                    # get most recent move
                ).to_dict()

            except:
                previous_role = {col: None for col in prev_cols}

            previous_roles.append(previous_role)

        rename_cols = {col: f'previous_{col}' for col in prev_cols}

        previous_roles = pd.DataFrame(previous_roles)
        previous_roles.rename(columns=rename_cols, inplace=True)

        def email_anonymizer(email_address: str):
            if '@' not in email_address:
                return "Insufficient data"

            _id, domain = email_address.split('@')

            if '.' in _id:
                id_parts = _id.split('.')
                id_parts = [x[0] + ('*' * 3) if len(x) else '' for x in id_parts]
                _id = '.'.join(id_parts)
            else:
                _id = _id[0] + ('*' * 3)

            if '.' in domain:
                domain_parts = domain.split('.')
                domain_parts[0] = domain_parts[0][0] + ('*' * 3)
                domain = '.'.join(domain_parts)

            return f'{_id}@{domain}'

        # Create final output DataFrame
        cols = [
            'career_id',
            'email_address',
            'email_address_anon',
            'confidence',
            'firstname',
            'lastname',
            'role',
            'company_id',
            'company_name',
            'linkedin_profile',
            'date_started',
            'date_ended',
            'previous_role',
            'previous_company_id',
            'previous_company_name',
            'previous_date_started',
            'previous_date_ended',
            'job_level',
            'previous_job_level',
            'location',
            'location_hub',
            'previous_location',
            'previous_location_hub',
            'raum',
            'previous_raum',
            'aum_bracket',
            'previous_aum_bracket',
            'firm_types',
            'previous_firm_types',
            'specialism',
            'previous_specialism',
            'job_category',
            'previous_job_category',
        ]

        # Add in previous roles
        try:
            moves = pd.concat([starts, previous_roles[rename_cols.values()]], axis=1)
            moves['email_address_anon'] = moves['email_address'].fillna('Insufficient data').apply(email_anonymizer)
            moves['location_hub'] = moves['location'].fillna('').apply(location.get_hub)
            moves['previous_location_hub'] = moves['previous_location'].fillna('').apply(location.get_hub)
            moves = moves[cols]
        except Exception as e:
            settings.LOGGER.error(str(e))
            return redirect('/data_science/moves/')

        for col in ['previous_date_started', 'previous_date_ended']:
            moves[col].fillna(datetime.datetime(1970, 1, 1), inplace=True)

        # Classify moves as internal (promotions) or external
        moves.loc[
            (moves['company_name'] == moves['previous_company_name']), 'move_type'
        ] = 'Internal'

        moves['move_type'].fillna('External', inplace=True)

        # Formatting of the output file
        for col in [
            'date_started',
            'date_ended',
            'previous_date_started',
            'previous_date_ended',
        ]:
            moves.loc[moves[col].notna(), col] = (
                moves[col].dt.month_name().astype(str).str[:3]
                + '-'
                + moves[col].dt.year.astype(str).str[-2:]
            )

        moves['date_ended'] = moves['date_ended'].replace({'Jan-70': 'Present'})

        for col in ['previous_date_started', 'previous_date_ended']:
            moves[col] = moves[col].replace({'Jan-70': ''})

        not_dones['linkedin_profile'] = (
            not_dones['linkedin_profile'].str.lower().str.split('/').str[-1].str.split('?').str[0]
        )

        not_dones = not_dones[
            (not_dones['linkedin_profile'].isin(moves['linkedin_profile'])) &
            (not_dones['date_ended']==datetime.date(1970, 1, 1)) &
            (not_dones['firm_types'].str.contains("HF"))
        ].sort_values(['linkedin_profile', 'date_started'], ascending=[True, False])

        not_dones.drop_duplicates('linkedin_profile', inplace=True)

        if not os.getenv("DOCKER"):
            t = threading.Thread(target=views.thread_validate_emails, args=[not_dones])
            t.setDaemon(True)
            t.start()

        # Clean up URLs for Excel
        for col in ['linkedin_profile']:
            moves[col] = (
                '=HYPERLINK("https://www.linkedin.com/in/'
                + moves[col]
                + '", "'
                + moves[col]
                + '")'
            )

        def obfuscate_confidence(value):
            if not value:
                return "Low"
            if 0 < value <= 0.33:
                return "Low"
            if 0.33 < value <= 0.66:
                return "Medium"
            if 0.66 < value:
                return "High"
            return "Low"

        moves["confidence"] = moves["confidence"].fillna(0).apply(obfuscate_confidence)
        moves.fillna('', inplace=True)
        moves.reset_index(drop=True, inplace=True)
        moves.drop_duplicates(["role", "company_name", "linkedin_profile"], inplace=True)

        name_cols = ["company_name", "previous_company_name"]
        companies_copy = moves[name_cols].groupby(name_cols).size().reset_index(name='counts')

        companies_copy = companies_copy[companies_copy["counts"]>=5]
        companies_copy.replace({"": None}, inplace=True)
        companies_copy.dropna(inplace=True)

        if companies_copy.shape[0]:
            for _, row in companies_copy.iterrows():
                moves.loc[
                    (moves['company_name'] == row['company_name']) &
                    (moves['previous_company_name'] == row['previous_company_name']) &
                    (moves['move_type'] != 'Internal'),
                'move_type'] = 'Acquisition'

        writer = pd.ExcelWriter(
            name,
            engine='xlsxwriter',
        )

        moves["cleaned_current_role"] = moves["role"].fillna("").apply(preprocessing.clean_string)

        # Remove any unwanted terms
        role_sw = stopwords.STOPWORDS["role"]
        settings.LOGGER.info(role_sw)

        try:
            role_sw.remove("office of")
            role_sw.remove("assistant")
        except:
            pass

        moves = dataframe.filter_df(moves, "cleaned_current_role", role_sw)
        moves = moves.reset_index(drop=True)
        moves = moves.drop(columns="cleaned_current_role")

        # Set previous_location_hub to location_hub where former Unavailable, latter not
        for i, row in moves.iterrows():
            if row["location_hub"] == "Unavailable" and row["previous_location_hub"] != "Unavailable":
                moves.loc[i, "location_hub"] = moves.loc[i, "previous_location_hub"]

        # Final fixes
        moves = moves[(moves["firm_types"].str.contains("HF Manager"))|(moves["previous_firm_types"].str.contains("HF Manager"))]
        moves = moves.drop_duplicates(
            subset=['firstname', 'lastname',"email_address","company_name", "date_started"], keep='last'
        )

        # Create DataFrame of company counts
        counts = (
            pd.DataFrame(moves['company_name'].value_counts())
            .reset_index()
            .rename(columns={'index': 'company', 'company_name': 'count'})
        )

        counts.to_excel(writer, index=False, sheet_name='Counts, by company')

        # Set column widths
        for column in counts:
            column_length = min(50, max(counts[column].astype(str).map(len).max(), len(column)))
            col_idx = counts.columns.get_loc(column)
            writer.sheets['Counts, by company'].set_column(col_idx, col_idx, column_length)

        # Freeze top row
        writer.sheets['Counts, by company'].freeze_panes(1, 0)

        # Lazy email validation
        emails_to_check = moves[["email_address", "firstname", "lastname", "company_name"]].replace({"": None}).dropna()
        emails_to_check["domain"] = emails_to_check["email_address"].apply(lambda x: x.split('@')[-1] if "@" in x else None)
        emails_to_check = emails_to_check[
            (emails_to_check['company_name'].str.lower().str.strip().str[0] !=
            emails_to_check['domain'].str.lower().str.strip().str[0])
        ]
        emails_to_check = emails_to_check.reset_index().rename(columns={'index': 'row_number'})
        emails_to_check["row_number"] = emails_to_check["row_number"].apply(lambda x: x+2)
        emails_to_check.to_excel(writer, index=False, sheet_name='Problems - emails')

        # Companies problem report
        companies_to_check = pd.concat([
            moves[["company_name", "firm_types"]],
            moves[["previous_company_name", "previous_firm_types"]]
        ]).replace({"": None})
        companies_to_check["company"] = companies_to_check["company_name"].fillna(companies_to_check["previous_company_name"])
        companies_to_check["types"] = companies_to_check["firm_types"].fillna(companies_to_check["previous_firm_types"])
        companies_to_check = companies_to_check[["company", "types"]]
        companies_to_check = companies_to_check[companies_to_check["types"].isna()].drop_duplicates().sort_values("company")
        companies_to_check = companies_to_check.reset_index().rename(columns={'index': 'row_number'})
        companies_to_check["row_number"] = companies_to_check["row_number"].apply(lambda x: x+2)
        companies_to_check.to_excel(writer, index=False, sheet_name='Problems - companies')

        # Job titles problem report
        roles_to_check = pd.concat([
            moves[["role", "job_category"]],
            moves[["previous_role", "previous_job_category"]]
        ]).replace({"": None})
        roles_to_check["title"] = roles_to_check["role"].fillna(roles_to_check["previous_role"])
        roles_to_check["category"] = roles_to_check["job_category"].fillna(roles_to_check["previous_job_category"])
        roles_to_check = roles_to_check[["title", "category"]]
        roles_to_check = roles_to_check[roles_to_check["category"].isna()].drop_duplicates().sort_values("title")
        roles_to_check = roles_to_check.reset_index().rename(columns={"index": "row_number"})
        roles_to_check["row_number"] = roles_to_check["row_number"].apply(lambda x: x+2)
        roles_to_check = roles_to_check[
            ~((roles_to_check["title"].str.contains("assistant", case=False)) |
            (roles_to_check["title"].str.contains("office of", case=False)))
        ]
        roles_to_check.to_excel(writer, index=False, sheet_name='Problems - roles')

        companies_sheet["aum_bracket"] = companies_sheet["aum_bracket"].fillna(0).apply(lambda x: fields.AUM_BRACKET[int(x)][1])
        companies_sheet.to_excel(writer, index=False, sheet_name=f'Companies - Master ({datetime.date.today().strftime("%Y-%-m-%d")})')

        for column in moves:
            moves = moves.rename(columns = {str(column): str(column).replace("_", " ").title()})

        moves.to_excel(writer, index=False, sheet_name='Moves')

        # Set column widths
        for column in moves:
            column_length = min(50, max(moves[column].astype(str).map(len).max(), len(column)))
            col_idx = moves.columns.get_loc(column)
            writer.sheets['Moves'].set_column(col_idx, col_idx, column_length)

        # Freeze top row
        writer.sheets['Moves'].freeze_panes(1, 0)

        writer.close()
    finally:
        MAKING_REPORTS.remove(name)

def newsletter_data(request):
    return render(request, 'data_science/newsletter.html')

def get_newsletter_data(request, pub):
    try:
        path = [x for x in os.listdir(settings.MEDIA_ROOT / 'newsletter_performance') if pub in x][0]
        return FileResponse(
            open(settings.MEDIA_ROOT / f'newsletter_performance/{path}', 'rb'),
            filename=f"{pub}-{datetime.date.today().strftime('%Y-%m-%d')}.xlsx"
        )
    except:
        return HttpResponseBadRequest('not found')
