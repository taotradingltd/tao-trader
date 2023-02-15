import datetime

from django.db.models import F, Value
from django.db.models.functions import Coalesce, NullIf

import django_tables2 as tables

from .fields import AUM_CATEGORY
from .models import Person, Company, Career

class PeopleTable(tables.Table):
    edit = tables.LinkColumn("edit_person", text="Edit", verbose_name="", args=[tables.A("pk")])
    firstname = tables.Column(verbose_name="First name")
    lastname = tables.Column(verbose_name="Last name")

    current_role = tables.TemplateColumn("""
    {{ record.current_role }}
    """, orderable=False)

    linkedin_profile = tables.TemplateColumn("""
        {% for value in record.linkedin_profile %}
            {% if value != "—" %}
                <a href="{{ value }}" target="_blank">{{ value|cut:"https://www.linkedin.com/in/"|truncatechars:30 }}</a>{% if record.linkedin_profile|length > 1 and forloop.counter < record.linkedin_profile|length %},{% endif %}
            {% else %}
                {{ value }}
            {% endif %}
        {% endfor %}
    """)

    email_address = tables.TemplateColumn("""
    {% for value in record.email_address %}
        {% if value != "—" %}
            <a href="mailto:{{ value }}" target="_blank">{{ value }}</a>{% if record.email_address|length > 1 and forloop.counter < record.email_address|length %},{% endif %}
        {% else %}
            {{ value }}
        {% endif %}
    {% endfor %}
    """, orderable=False)

    career_history = tables.LinkColumn("career", text="View career history", verbose_name="", args=[tables.A("pk")], orderable=False)

    def order_linkedin_profile(self, queryset, is_descending):
        queryset = queryset.annotate(
            url=F("personsocial__value")
        ).order_by(("-" if is_descending else "") + "url")

        return (queryset, True)

    class Meta:
        model = Person
        fields = ("edit", "id", "firstname", "lastname","email_address", "current_role", "linkedin_profile", "date_modified", "date_added")

class CareerHistoryTable(tables.Table):
    company = tables.TemplateColumn("""
    {% if record.company_obj.pk > 0 %}
        <a href="/data/current_employees/{{ record.company_obj.pk }}">{{ record.company }}</a>
    {% else %}
        {{ record.company }}
    {% endif %}
    """, verbose_name="Company", orderable=False)

    role = tables.Column(accessor="role", verbose_name="Role")

    date_started = tables.TemplateColumn("""
    {% if record.start_date_is_known %}
        {{ record.date_started|date:"M Y" }}
    {% else %}
        Unknown
    {% endif %}
    """, verbose_name="From")

    date_ended = tables.TemplateColumn("""
    {% if record.start_date_is_known and not record.end_date_is_known %}
        Present
    {% elif record.start_date_is_known and record.end_date_is_known %}
        {{ record.date_ended|date:"M Y" }}
    {% else %}
        Unknown
    {% endif %}
    """, verbose_name="To")

    class Meta:
        model = Career
        fields = ("company", "role", "date_started", "date_ended")

class CompaniesTable(tables.Table):
    edit = tables.LinkColumn("edit_company", text="Edit", verbose_name="", args=[tables.A("pk")])
    merge = tables.LinkColumn("company_merge", text="Merge", verbose_name="", args=[tables.A("pk")])
    company_id = tables.Column(accessor="pk", verbose_name="ID")

    company_name = tables.TemplateColumn("""
    {% if record.name %}
        {{ record.name }}
    {% else %}
        {{ record.aliases.0 }}
    {% endif %}
    """, verbose_name="Company name")

    aum_category = tables.Column(accessor="aum_category", verbose_name="AUM")
    firm_types = tables.TemplateColumn('{{ record.firm_types|join:", " }}')

    website = tables.TemplateColumn("""
    {{ record.website|safe }}
    """, verbose_name="Website")

    linkedin_url = tables.TemplateColumn("""
    {{ record.linkedin_url|safe }}
    """, verbose_name="Linkedin")

    current_employees = tables.TemplateColumn("""
    <a href="/data/current_employees/{{ record.pk }}">View current employees</a>
    """, verbose_name="", orderable=False)

    def order_linkedin_url(self, queryset, is_descending):
        queryset = queryset.annotate(
            url=F("companylinkedin__value")
        ).order_by(("-" if is_descending else "") + "url")

        return (queryset, True)

    def order_company_name(self, queryset, is_descending):
        queryset = queryset.annotate(
            company_name=NullIf(Coalesce("name", "career__company"), Value(""))
        ).order_by(F("company_name").asc(nulls_last=True))

        if is_descending:
            return (queryset.reverse(), True)
        return (queryset, True)

    def order_firm_types(self, queryset, is_descending):
        queryset = queryset.annotate(
            type=F("companyfirmtype__firm_type__firm_type")
        ).order_by(("-" if is_descending else "") + "type")

        return (queryset, True)

    def render_aum_category(self, value):
        return AUM_CATEGORY[int(value)][1]

    class Meta:
        model = Company
        fields = ("edit", "merge", "company_id", "company_name", "aum_category", "firm_types", "website", "linkedin_url")

class RecentMovesTable(tables.Table):
    person_first_name = tables.Column(accessor="person.firstname", verbose_name="First name")
    person_last_name = tables.Column(accessor="person.lastname", verbose_name="Last name")

    person_linkedin = tables.TemplateColumn("""
    {% for value in record.person.linkedin_profile %}
        {% if value != "-" %}
            <a href="{{ value }}" target="_blank">{{ value|cut:"https://www.linkedin.com/in/"|truncatechars:30 }}</a>
        {% else %}
            {{ value }}
        {% endif %}
    {% endfor %}
    """, verbose_name="Linkedin profile")

    company_name = tables.TemplateColumn("""
    {% if record.company_obj.pk > 0 %}
        <a href="/data/current_employees/{{ record.company_obj.pk }}">{{ record.company }}</a>
    {% else %}
        {{ record.company }}
    {% endif %}
    """, verbose_name="Company")

    role = tables.Column(accessor="role", verbose_name="Role")
    date_started = tables.Column(accessor="date_started", verbose_name="Month started")
    date_modified = tables.Column(accessor="date_modified", verbose_name="Last updated")
    career_history = tables.LinkColumn("career", text="View career history", verbose_name="", args=[tables.A("person.pk")])

    def render_date_started(self, value):
        if value == datetime.date(1970, 1, 1):
            return "Unknown"
        return f'{value.strftime("%b")} {value.year}'

    def order_person_linkedin(self, queryset, is_descending):
        queryset = queryset.annotate(
            url=F("person__personsocial__value")
        ).order_by(("-" if is_descending else "") + "url")

        return (queryset, True)

    class Meta:
        model = Career
        fields = ("person_first_name", "person_last_name", "person_linkedin", "company_name", "role", "date_started", "date_modified")
