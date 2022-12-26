import datetime
import json
import os
import pdfkit
import re
import requests
import shutil
import tempfile
import threading
import time
import urllib

import django_tables2 as tables
import pandas as pd

from bs4 import BeautifulSoup

from django.conf import settings
from django.contrib.auth.decorators import login_required, user_passes_test
from django.db import connections
from django.db.models import F, IntegerField, Q, Value
from django.db.models.functions import NullIf
from django.http.response import JsonResponse, FileResponse
from django.shortcuts import render, redirect
from django.urls import reverse
from django.views.defaults import page_not_found
from django.views.generic.edit import CreateView, FormView, UpdateView

from gfm_utils.text import preprocessing

from .fields import EMAIL_FORMAT_LAMBDA_MAP
from .forms import CompanyMergeForm, UploadForm, CompanyForm, CompanyUpdateForm, PersonForm, PersonUpdateForm, ScrapingForm, ReportSearchForm
from .models import CareerJobCategory, CompanyContact, CompanyFirmType, FirmType, Person, Company, Career, PersonEmail, SocialMedia, JobCategory, CompanyFormat, CompanyLinkedin, CompanyRelation, CompanyWebsite
from .tables import PeopleTable, CompaniesTable, CareerHistoryTable, RecentMovesTable
from .utils import csvs_to_db, json_to_db, UPLOAD_RUNNING

from articles.models import Article
from data_science.views import MAKING_REPORTS

EMAIL_COLLECTOR_RUNNING = False

class PersonCreateView(CreateView):
    model = Person
    form_class = PersonForm
    template_name = "data/form.html"
    success_url = "/data/people"

    def post(self, request, *args, **kwargs):
        if "cancel" in request.POST:
            url = self.success_url
            return redirect(url)
        else:
            return super(PersonCreateView, self).post(request, *args, **kwargs)

class PersonUpdateView(UpdateView):
    model = Person
    form_class = PersonUpdateForm
    template_name = "data/form.html"
    success_url = "/data/people"

    def post(self, request, *args, **kwargs):
        if "cancel" in request.POST:
            url = self.success_url
            return redirect(url)
        else:
            return super(PersonUpdateView, self).post(request, *args, **kwargs)

class CompanyCreateView(CreateView):
    model = Company
    form_class = CompanyForm
    template_name = "data/form.html"
    success_url = "/data/companies"

    def post(self, request, *args, **kwargs):
        if "cancel" in request.POST:
            url = self.success_url
            return redirect(url)
        elif "remove_parent" in request.POST:
            CompanyRelation.objects.get(child=self.instance).delete()
            return redirect(request.META['HTTP_REFERER'])
        else:
            return super(CompanyCreateView, self).post(request, *args, **kwargs)

class CompanyUpdateView(UpdateView):
    model = Company
    form_class = CompanyUpdateForm
    template_name = "data/form.html"
    success_url = "/data/companies"

    def get(self, request, *args, **kwargs):
        if int(kwargs.get("pk")) < 1:
            return page_not_found(request, None)
        return super().get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        if "cancel" in request.POST:
            url = self.success_url
            return redirect(url)
        else:
            return super(CompanyUpdateView, self).post(request, *args, **kwargs)

class MultipleFileFormView(FormView):
    form_class = UploadForm
    template_name = "data/form.html"
    success_url = "/data/people"

    def post(self, request, *args, **kwargs):
        form_class = self.get_form_class()
        form = self.get_form(form_class)
        files = request.FILES.getlist("files")
        if form.is_valid():
            paths = []
            for f in files:
                fname = f.temporary_file_path().split("/")[-1]
                dest = f"{settings.TMP_DIR}/{fname}"

                shutil.copyfile(f.temporary_file_path(), dest)
                paths.append(dest)

            json_paths = [path for path in paths if path.split(".")[-1].lower() == "json"]
            csv_paths = [path for path in paths if path.split(".")[-1].lower() == "csv"]

            for group in [json_paths, csv_paths]:
                if len(group):
                    if group[0].split(".")[-1].lower() == "json":
                        args = [group]
                        t = threading.Thread(target=json_to_db, args=args)
                        t.start()
                    elif group[0].split(".")[-1].lower() == "csv":
                        args = [group]
                        t = threading.Thread(target=csvs_to_db, args=args)
                        t.start()
            return self.form_valid(form)
        else:
            return self.form_invalid(form)

class ScrapingFormView(FormView):
    form_class = ScrapingForm
    template_name = "data/form.html"
    success_url = "/data/people"

@login_required
def index(request):
    return redirect(reverse(people))

@login_required
def people(request):
    message = ""

    search_terms = [
        "firstname",
        "lastname",
        "email",
        "role",
        "company",
        "profile",
        "jobcategory"
    ]

    p = Person.objects.all()
    s = False

    if request.GET:
        for term in search_terms:
            value = request.GET.get(term)
            if value:
                if term == "firstname":
                    p = p.filter(firstname__icontains=value)
                    s = True
                if term == "lastname":
                    p = p.filter(lastname__icontains=value)
                    s = True
                if term == "email":
                    p = p.filter(personemail__value__icontains=value)
                    s = True
                if term == "role":
                    p = p.filter(career__date_ended__year="1970", career__date_started__year__gt="1970", career__role__icontains=value)
                    s = True
                if term == "company":
                    cleaned_name = preprocessing.clean_name(value, 'company', True, False)
                    companies = Company.objects.filter(
                        Q(cleaned_name__icontains=cleaned_name) | Q(career__company__icontains=cleaned_name)
                    ).exclude(pk=-1)
                    p = p.filter(
                        career__date_ended__year="1970",
                        career__date_started__year__gt="1970",
                        career__company_obj__in=companies,
                    )
                    s = True
                if term == "profile":
                    p = p.filter(personsocial__value__icontains=value, personsocial__social_media=SocialMedia.objects.get(pk=1))
                    s = True
                if term == "jobcategory":
                    p = p.filter(career__careerjobcategory__jobcategory=value, career__date_ended__year="1970")
                    s = True

    if s:
        p = p.distinct()
        message = f"{len(p)} results found."

    table = PeopleTable(p, order_by=["-id"])
    tables.config.RequestConfig(request, paginate={"per_page": 25}).configure(table)
    return render(request, "data/people_table.html", {"table": table, "message": message, "jobcategories": JobCategory.objects.all()})

@login_required
def company(request):
    message = ""

    search_terms = [
        "company_id",
        "name",
        "profile",
        "firm_type",
    ]

    c = Company.objects.filter(pk__gt=0).annotate(nulled_name=NullIf("name", Value("")))\
        .order_by("-aum_category", F("nulled_name").asc(nulls_last=True), "pk")
    s = False

    if request.GET:
        for term in search_terms:
            value = request.GET.get(term)
            if value:
                if term == "company_id":
                    try:
                        c = c.filter(pk=value)
                    except:
                        continue
                    break
                if term == "name":
                    cleaned_value = preprocessing.clean_name(value, 'company', True, False)
                    c1 = c.filter(name__icontains=value).annotate(relevance=Value(1, output_field=IntegerField()))
                    # c2 = c.filter(career__company__icontains=value).annotate(relevance=Value(2, output_field=IntegerField()))
                    c3 = c.filter(cleaned_name__icontains=cleaned_value).annotate(relevance=Value(3, output_field=IntegerField()))
                    c = (c1 | c3).order_by("relevance")
                    # c = (c1 | c2 | c3).order_by("relevance")
                if term == "profile":
                    c = c.filter(companylinkedin__value__icontains=value)
                if term == "firm_type":
                    c = c.filter(companyfirmtype__company__in=c, companyfirmtype__firm_type__pk=value)

    table = CompaniesTable(c.distinct())
    tables.config.RequestConfig(request, paginate={"per_page": 25}).configure(table)
    return render(
        request,
        "data/company_table.html",
        {
            "table": table,
            "message": message,
            "firm_types": FirmType.objects.filter(is_subtype=0)
        }
    )

@login_required
def career(request, pk):
    person = Person.objects.get(pk=pk)
    name = f"{person.firstname} {person.lastname}"
    url = person.linkedin_profile[0]
    table = CareerHistoryTable(Career.objects.filter(person=pk), order_by=["-date_started"])
    tables.config.RequestConfig(request, paginate={"per_page": 25}).configure(table)

    return render(request, "data/career_table.html", {"table": table, "person_name": name, "linkedin_profile": url})

@login_required
def recent_updates(request):
    message = ""
    search_terms = [
        "firstname",
        "lastname",
        "role",
        "company",
        "profile",
        "firm_type"
    ]

    td = datetime.date.today().replace(day=1) - datetime.timedelta(weeks=8)
    c = Career.objects.filter(date_started__gte=td, date_ended="1970-01-01")
    s = False

    if request.GET:
        for term in search_terms:
            value = request.GET.get(term)
            if value:
                if term == "firstname":
                    c = c.filter(person__firstname__icontains=value)
                    s = True
                if term == "lastname":
                    c = c.filter(person__lastname__icontains=value)
                    s = True
                if term == "role":
                    c = c.filter(date_ended="1970-01-01", role__icontains=value)
                    s = True
                if term == "company":
                    c1 = c.filter(date_ended="1970-01-01", company_obj__name__icontains=value)
                    c2 = c.filter(date_ended="1970-01-01", company__icontains=value)
                    c = c1 | c2
                    s = True
                if term == "profile":
                    c = c.filter(
                        person__personsocial__value__icontains=value,
                        person__personsocial__social_media=SocialMedia.objects.get(pk=1)
                    )
                    s = True
                if term == "firm_type":
                    c = c.filter(company_obj__companyfirmtype__firm_type__pk=value)
                    s = True

    c = c.distinct()
    if s:
        message = f"{len(c)} results found."

    firm_types = FirmType.objects.filter(is_subtype=0)

    table = RecentMovesTable(c, order_by=["-date_started", "-date_modified"])
    tables.config.RequestConfig(request, paginate={"per_page": 25}).configure(table)
    return render(request, "data/recent_moves.html", {"table": table, "message": message, "firm_types": firm_types})

@login_required
def current_employees(request, pk):
    if int(pk) < 1:
        return page_not_found(request, None)

    search_terms = [
        "firstname",
        "lastname",
        "role",
        "profile",
        "jobcategory",
        "showall",
        "fuzz",
    ]

    # By default, only show career_ids of C-suite only
    c_ids = CareerJobCategory.objects.filter(jobcategory_id=13).values_list("career_id", flat=True)

    company = [Company.objects.get(pk=pk)]
    matched_name = company[0].name if company[0].name else company[0].aliases[0]

    if request.GET:
        if request.GET.get("fuzz"):
            cleaned_name = preprocessing.clean_name(matched_name, 'company', True, False)

            extra_companies = Company.objects.filter(
                Q(name=matched_name) | Q(cleaned_name__icontains=cleaned_name)\
                    | Q(career__company__icontains=cleaned_name)
            ).exclude(pk=-1)

            company = company + [c for c in extra_companies]

    if c_ids.count() != 0:
        # C-suite found
        c = Career.objects.filter(company_obj__in=company, date_ended="1970-01-01", pk__in=c_ids)
    else:
        # No C-suite found
        c = Career.objects.filter(company_obj__in=company, date_ended="1970-01-01")

    if request.GET:
        if request.GET.get("showall"):
            c = Career.objects.filter(company_obj__in=company, date_ended="1970-01-01")

    if request.GET:
        for term in search_terms:
            value = request.GET.get(term)
            if value:
                if term == "firstname":
                    c = c.filter(person__firstname__icontains=value)
                if term == "lastname":
                    c = c.filter(person__lastname__icontains=value)
                if term == "role":
                    c = c.filter(role__icontains=value)
                if term == "profile":
                    c = c.filter(
                        person__personsocial__value__icontains=value,
                        person__personsocial__social_media=SocialMedia.objects.get(pk=1)
                    )
                if term == "jobcategory":
                    c = c.filter(careerjobcategory__jobcategory=value, date_ended="1970-01-01")

    table = RecentMovesTable(c.distinct(), order_by=["role", "person_last_name", "date_started"])
    tables.config.RequestConfig(request, paginate={"per_page": 25}).configure(table)

    return render(
        request,
        "data/current_employees.html",
        {
            "table": table,
            "company": matched_name,
            "count": len(table.rows),
            "jobcategories": JobCategory.objects.all()
        }
    )

def brightdata_to_db(csv_paths):
    for csv_path in csv_paths:
        csv = pd.read_csv(csv_path, encoding="utf-8-sig")
        csvs_to_db(csv)

def preprocess(text):
    pattern = re.compile("[^\w\s\-\"\+]+")
    return pattern.sub("", text).strip()

@login_required
@user_passes_test(lambda u: u.is_staff)
def statistics(request):
    table_stats = {
        "Person": Person.objects.count(),
        "Company": Company.objects.count(),
        "Career": Career.objects.count(),
        "CareerJobCategory": CareerJobCategory.objects.count(),
        "CompanyFirmType": CompanyFirmType.objects.count(),
    }

    with connections["default"].cursor() as cursor:
        cursor.execute("""
        SELECT firmtype.value, COUNT(firmtype.id)
        FROM career
        INNER JOIN company ON company.id=career.company_id
        INNER JOIN company_firmtype ON company.id=company_firmtype.company_id
        INNER JOIN firmtype ON firmtype.id=company_firmtype.firmtype_id
        WHERE company_firmtype.firmtype_id IN (1, 2, 3) AND career.date_started >= DATE_SUB(curdate(), INTERVAL 1 MONTH)
        GROUP BY firmtype.value;
        """)
        moves_lastmonth = cursor.fetchall()
        moves_lastmonth = dict((x, y) for x, y in moves_lastmonth)

        cursor.execute("""
        SELECT firmtype.value, COUNT(firmtype.id)
        FROM career
        INNER JOIN company ON company.id=career.company_id
        INNER JOIN company_firmtype ON company.id=company_firmtype.company_id
        INNER JOIN firmtype ON firmtype.id=company_firmtype.firmtype_id
        WHERE company_firmtype.firmtype_id IN (1, 2, 3) AND career.date_started < DATE_SUB(curdate(), INTERVAL 1 MONTH)
            AND career.date_started >= DATE_SUB(curdate(), INTERVAL 2 MONTH)
        GROUP BY firmtype.value;
        """)
        moves_monthbefore = cursor.fetchall()
        moves_monthbefore = dict((x, y) for x, y in moves_monthbefore)

    for key in ["HF Manager", "PM Manager", "Allocator"]:
        if key not in moves_lastmonth:
            moves_lastmonth[key] = 0
        if key not in moves_monthbefore:
            moves_monthbefore[key] = 0

    move_stats = {
        "Incoming HF moves last month": moves_lastmonth["HF Manager"],
        "Incoming HF moves month before": moves_monthbefore["HF Manager"],
        "Incoming PE moves last month": moves_lastmonth["PM Manager"],
        "Incoming PE moves month before": moves_monthbefore["PM Manager"],
        "Incoming Allocator moves last month": moves_lastmonth["Allocator"],
        "Incoming Allocator moves month before": moves_monthbefore["Allocator"],
    }

    state_stats = {
        "Collecting emails": EMAIL_COLLECTOR_RUNNING,
        "Uploading JSON data file": UPLOAD_RUNNING,
        "Making reports": bool(len(MAKING_REPORTS))
    }

    context = {
        "table_stats": table_stats,
        "move_stats": move_stats,
        "state_stats": state_stats,
    }
    return render(request, "data/statistics.html", context=context)

@login_required
@user_passes_test(lambda u: u.is_staff)
def clean(request):
    if request.POST:
        if "cancel" in request.POST:
            return redirect("people")

        if "clean" in request.POST:
            t = threading.Thread(target=clean_db)
            t.setDaemon(True)
            t.start()
            return redirect("people")

    return render(request, "data/clean.html")

def clean_db():
    settings.LOGGER.info("Cleaning database")
    # Write and call helper functions to fill this function
    settings.LOGGER.info("Database cleaned")

@login_required
def get_company_ids(request):
    gfm_id = request.GET["gfm_id"]
    cleaned_name = preprocessing.clean_name(gfm_id, "company", True, False)
    companies = list(
        (Company.objects.filter(gfm_company_id__icontains=gfm_id) | Company.objects.filter(cleaned_name__icontains=cleaned_name))[:250].values_list("pk", "gfm_company_id", "name")
    )
    companies = companies

    return JsonResponse({"company_ids": [{ "id": c[0], "gfm_id": c[1], "name": c[2] } for c in companies]})

@login_required
def get_company_names(request):
    name = request.GET["name"]
    cleaned_name = preprocessing.clean_name(name, "company", True, False)
    companies = list(
        (Company.objects.filter(name__icontains=name)|Company.objects.filter(cleaned_name__icontains=cleaned_name))
    .values_list("pk", "name"))

    return JsonResponse({"names": [{"name": c[1], "id": c[0] } for c in companies]})

def _call_apollo_bulk_match(payload_dict: dict):
    url = "https://api.apollo.io/api/v1/people/bulk_match"
    payload_dict["api_key"] = os.environ["APOLLO_KEY"]
    payload = json.dumps(payload_dict)

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
    }

    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        return response.json()
    except requests.exceptions.JSONDecodeError:
        return {}

def thread_validate_emails(df):
    global EMAIL_COLLECTOR_RUNNING

    if EMAIL_COLLECTOR_RUNNING:
        settings.LOGGER.warning("Email collector already running")
        return

    EMAIL_COLLECTOR_RUNNING = True

    try:
        chunks = [df.iloc[i:i+10] for i in range(0, len(df), 10)]

        for chunk in chunks:
            inp = {
                "details": [
                    {
                        "id": row["person_id"],
                        "first_name": row["firstname"],
                        "last_name": row["lastname"],
                        "organization_name": row["company_name"],
                    }
                for _, row in chunk.iterrows()]
            }

            emails = _validate_emails(inp)

            for person_id in emails:
                if "email" in emails[person_id]:
                    if emails[person_id]["email"]:
                        df.loc[df["person_id"]==person_id, "email_address"] = emails[person_id]["email"]
            time.sleep(120)
    finally:
        EMAIL_COLLECTOR_RUNNING = False

def _validate_emails(data: dict):
    people = data["details"]

    if len(people) > 10:
        settings.LOGGER.warning("Can only do 10 at a time.")
        people = people[:10]

    payload_dict = {
        "details": [
            {
                "first_name": p["first_name"],
                "last_name": p["last_name"],
                "organization_name": p["organization_name"]
            } for p in people
        ]
    }

    ids = [p["id"] for p in people]

    response = _call_apollo_bulk_match(payload_dict)

    try:
        response = response["matches"]
    except:
        return {}

    response = {
            _id: match
        for _id, match in zip(ids, response)
    }

    ids = list(response.keys())

    for _id in ids:
        if response[_id]:
            response[_id] = {
                "email": response[_id]["email"],
                "status": response[_id]["email_status"],
                "confidence": response[_id]["extrapolated_email_confidence"]
            }
        else:
            del response[_id]

    for person_id in response:
        if response[person_id]["status"] == "verified":
            response[person_id]["confidence"] = 1

        if response[person_id]["confidence"] is None:
            response[person_id]["confidence"] = 0

        if response[person_id]["email"]:
            try:
                e = PersonEmail.objects.get(
                    person=Person.objects.get(pk=person_id),
                    source="Apollo"
                )
                e.confidence = response[person_id]["confidence"]
                e.value = response[person_id]["email"]
            except:
                e = PersonEmail(
                    person=Person.objects.get(pk=person_id),
                    value=response[person_id]["email"],
                    confidence=response[person_id]["confidence"],
                    source="Apollo"
                )

            e.save()

    return response

@login_required
def validate_email(request):
    try:
        post = json.loads(request.body)
    except:
        return JsonResponse({"ERROR": "poorly formed request"}, status=400)

    p = []
    for person in post["details"]:
        if not person["email_address"].strip():
            p.append(person)
    post["details"] = p

    if not len(post["details"]):
        return JsonResponse({"SUCCESS": "email already exists"}, status=200)
    elif len(post["details"]) >= 1:
        response = _validate_emails(post)

    if response:
        return JsonResponse(response, status=200)
    return JsonResponse({"ERROR": f"failed to find email"}, status=500)

@login_required
def company_merge(request, pk):
    form = CompanyMergeForm()
    c = Company.objects.get(pk=pk)
    name = c.name if c.name else c.name_appearances[0][0]
    context = {"form": form, "message": f"Who should {c.gfm_company_id} ({name}) be merged with?"}

    if request.POST:
        if "cancel" in request.POST:
            return redirect("companies")
        company_id = request.POST["company_id"]

        if company_id == pk:
            context["message"] += '<h5 style="text-align:center;color:red;">Can\'t merge a company with itself.</h5>'
            return render(request, "data/company_merge.html", context)

        merge_to = Company.objects.get(pk=company_id)

        models_to_change = [
            CompanyContact,
            CompanyFirmType,
            CompanyFormat,
            CompanyLinkedin,
            CompanyWebsite,
        ]

        for model in models_to_change:
            q = model.objects.filter(company=c)
            for _q in q: _q.company = merge_to
            for obj in q:
                try:
                    obj.save()
                except:
                    # Duplicate object
                    obj.delete()
                    continue

        # Treat career differently
        q = Career.objects.filter(company_obj=c)
        for _q in q: _q.company_obj = merge_to
        for obj in q:
            try:
                obj.save()
            except:
                # Duplicate object
                obj.delete()
                continue

        # Treat company relations differently
        q = CompanyRelation.objects.filter(child=c)
        new_relations = [CompanyRelation(parent=_q.parent, child=merge_to) for _q in q]
        CompanyRelation.objects.bulk_create(new_relations, ignore_conflicts=True)
        q.delete()
        c.delete()

        return redirect(reverse("companies"))

    return render(request, "data/company_merge.html", context)

@login_required
def report_builder(request):
    form = ReportSearchForm()
    context = {"form": form}

    if request.POST:
        company_name = request.POST["company_name"]
        return redirect(report, pk=company_name)

    return render(request, "data/form.html", context)

@login_required
def report(request, pk):
    company = Company.objects.get(pk=pk)
    articles = Article.objects.filter(title__icontains=company.name) | Article.objects.filter(content__icontains=company.name)
    people = Career.objects.filter(company_obj=company, date_ended="1970-01-01").distinct().order_by("role", "person__lastname")

    if request.POST:
        if "save-report" in request.POST:
            # Verify emails
            emails = list(set([item for sublist in [item.person.email_address for item in people] for item in sublist]))
            try:
                emails.remove("—")
            except ValueError:
                pass

            emails = emails[:5]

            with open(f"{settings.BASE_DIR}/static/style.css", "r") as f:
                css = f.read()
                css = css.replace("/* gfm-form width */\n    width: 70%;\n    border-radius:", "width: 80%;border: 3px solid #333;border-radius:")
                css = re.sub("background-color:.*;", "", css)
            source = request.POST["sourceField"]
            soup = BeautifulSoup(source, "html.parser")

            # Remove unused divs
            body = soup.find("div", {"id": "content"})

            for button in body.find_all("button"):
                button.decompose()

            output = '<meta charset="utf-8"><style type="text/css">' + css + "</style>" + body.prettify()

            with tempfile.NamedTemporaryFile(prefix=f"report-user_{request.user.pk}-", suffix=".pdf") as f:
                success = pdfkit.from_string(output, output_path=f.name, options={
                        "margin-top": "20mm",
                        "margin-bottom": "20mm",
                        "margin-right": "0",
                        "margin-left": "0",
                    }
                )

                if success:
                    return FileResponse(open(f.name, "rb"))
    if request.GET:
        search_terms = [
            "firstname",
            "lastname",
            "role",
            "title",
            "source",
            "publish_date",
        ]
        for term in search_terms:
            value = request.GET.get(term)
            if value:
                if term == "firstname":
                    people = people.filter(person__firstname__value__icontains=value)
                if term == "lastname":
                    people = people.filter(person__lastname__value__icontains=value)
                if term == "role":
                    people = people.filter(role__icontains=value)
                if term == "title":
                    articles = articles.filter(title__icontains=value)
                if term == "source":
                    articles = articles.filter(source__icontains=value)
                if term == "publish_date":
                    articles = articles.filter(publish_date__gte=value)

    context = {"articles": articles.order_by("-publish_date"), "company_name": company.name, "contacts": people, "generated_at": str(datetime.datetime.now())}
    return render(request, "data/report.html", context)
