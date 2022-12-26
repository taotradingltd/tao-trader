from datetime import date

from django.db import models
from django.db.models import Count
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _

import nltk
STOP_WORDS = nltk.download('stopwords')
# STOP_WORDS = stopwords.words("english")

class FirmTypes(models.TextChoices):
    HF_MANAGER = "HF Manager", _("HF Manager")
    PM_MANAGER = "PM Manager", _("PM Manager")
    ALLOCATOR = "Allocator", _("Allocator")
    HF_SERVICE_PROVIDER = "HF Service Provider", _("HF Service Provider")
    PM_FUND_SERVICE_PROVIDER = "PM Fund Service Provider", _("PM Fund Service Provider")

class SubTypes(models.TextChoices):
    EQUITY = "Equity", _("Equity")
    CREDIT = "Credit", _("Credit")
    GLOBAL_MACRO = "Global macro", _("Global macro")
    RELATIVE_VALUE = "Relative value", _("Relative value")
    CTA = "CTA", _("CTA")
    EVENT_DRIVEN = "Event-driven", _("Event-driven")
    MULTI_STRATEGY = "Multi-strategy", _("Multi-strategy")
    DIGITAL_ASSETS = "Digital assets", _("Digital assets")
    OTHER = "Other", _("Other")
    PRIVATE_EQUITY = "Private equity", _("Private equity")
    VENTURE_CAPITAL = "Venture capital", _("Venture capital")
    REAL_ESTATE = "Real estate", _("Real estate")
    INFRASTRUCTURE = "Infrastructure", _("Infrastructure")
    PRIVATE_CREDIT_DEBT = "Private credit/debt", _("Private credit/debt")
    BANK_PLATFORM_WEALTH_MANAGER = "Bank platform/Wealth manager", _("Bank platform/Wealth manager")
    CORPORATE_INVESTOR = "Corporate investor", _("Corporate investor")
    ENDOWMENT_FOUNDATION = "Endowment/Foundation", _("Endowment/Foundation")
    FUND_OF_FUNDS = "Fund of funds", _("Fund of funds")
    SOVEREIGN_WEALTH_FUND = "Sovereign wealth fund", _("Sovereign wealth fund")
    INSURANCE = "Insurance", _("Insurance")
    PENSION = "Pension", _("Pension")
    FAMILY_OFFICE = "Family office", _("Family office")
    INVESTMENT_CONSULTANT = "Investment consultant", _("Investment consultant")
    ADMINISTRATOR = "Administrator", _("Administrator")
    LEGAL_AND_COMPLIANCE = "Legal and compliance", _("Legal and compliance")
    PRIME_BROKER = "Prime broker", _("Prime broker")
    TAX_AND_AUDIT = "Tax and audit", _("Tax and audit")
    CUSTODY = "Custody", _("Custody")
    TECHNOLOGY = "Technology", _("Technology")
    ADVISORY = "Advisory", _("Advisory")
    FINANCING_SERVICES = "Financing services", _("Financing services")
    QUANTITATIVE = "Quantitative", _("Quantitative")

class JobCategories(models.TextChoices):
    FRONT_OFFICE_AND_INVESTMENT = "Front office and investment", _("Front office and investment")
    LEGAL_AND_COMPLIANCE = "Legal and compliance", _("Legal and compliance")
    OPERATIONS_AND_FINANCE = "Operations and finance", _("Operations and finance")
    SALES_IR = "Sales/IR", _("Sales/IR")
    TECHNOLOGY = "Technology", _("Technology")

class EmailFormats(models.TextChoices):
    FIRST = "first.last", _("first.last")
    SECOND = "first", _("first")
    THIRD = "flast", _("flast")
    FOURTH = "fl", _("fl")
    FIFTH = "firstlast", _("firstlast")
    SIXTH = "f.last", _("f.last")
    SEVENTH = "first_last", _("first_last")
    EIGHTH = "firstl", _("firstl")
    NINTH = "last", _("last")
    TENTH = "f.l", _("f.l")
    ELEVENTH = "lastfirst", _("lastfirst")
    TWELFTH = "lastf", _("lastf")
    THIRTEENTH = "last.first", _("last.first")
    FOURTEENTH = "first.l", _("first.l")
    FIFTEENTH = "fla", _("fla")

class Specialisms(models.TextChoices):
    NONE = "—", _("—")

    ACTIVIST = "Activist", _("Activist")
    CREDIT = "Credit", _("Credit")
    DEBT = "Debt", _("Debt")
    DIGITAL = "Digital Assets", _("Digital Assets")
    EQUITY = "Equity", _("Equity")
    EVENT = "Event-driven", _("Event-driven")
    FUTURES = "Futures", _("Futures")
    MACRO = "Macro", _("Macro")
    MULTI = "Multi-strategy", _("Multi-strategy")
    NICHE = "Niche", _("Niche")
    QUANT = "Quant", _("Quant")
    VARIOUS = "Various", _("Various")

class FirmType(models.Model):
    firm_type = models.CharField(choices=FirmTypes.choices + SubTypes.choices, max_length=50, unique=True, db_column="value")
    is_subtype = models.BooleanField(default=1, db_column="subtype")

    class Meta:
        db_table = "firmtype"

class JobCategory(models.Model):
    category = models.CharField(choices=JobCategories.choices, max_length=50, unique=True, db_column="value")

    class Meta:
        db_table = "jobcategory"

class SocialMedia(models.Model):
    name = models.CharField(max_length=50, unique=True, db_column="name")
    domain = models.CharField(max_length=255, db_column="domain")

    class Meta:
        unique_together = (("name", "domain"),)
        db_table = "social"

class Company(models.Model):
    name = models.CharField(max_length=255, db_column="name", db_index=True,)
    cleaned_name = models.CharField(max_length=255, db_column="cleaned_name", db_index=True,)
    aum_category = models.IntegerField(default=0, db_column="aum_category")
    raum = models.FloatField(default=0, db_column="raum")
    aum_bracket = models.IntegerField(default=0, db_column="aum_bracket")
    specialism = models.CharField(choices=Specialisms.choices, null=True, default=None, max_length=50, db_column="specialism")
    date_modified = models.DateTimeField(auto_now=True)
    date_added = models.DateTimeField(auto_now_add=True)

    gfm_company_id = models.CharField(max_length=50, default="", db_column="gfm_company_id")

    @cached_property
    def aliases(self):
        aliases = Career.objects.filter(company_obj=self)\
            .annotate(count=Count("company")).order_by("-count")
        if len(aliases):
            return [a.company for a in aliases]
        return ["—"]

    @cached_property
    def name_appearances(self):
        if self.name:
            return [(self.name, "1")]
        objs = Career.objects.filter(company_obj=self).values("company")\
            .annotate(count=Count("company")).order_by("-count")
        if len(objs):
            return [(o["company"], o["count"]) for o in objs]
        return []

    @cached_property
    def firm_types(self):
        types = CompanyFirmType.objects.filter(company=self, firm_type__is_subtype=0).order_by("firm_type__firm_type")
        if len(types):
            return [t.firm_type.firm_type for t in types]
        return ["—"]

    @cached_property
    def website(self):
        websites = CompanyWebsite.objects.filter(company=self).order_by("value")
        if len(websites):
            app = "" if len(websites) == 1 else f" (of {len(websites)} websites)"
            return f'<a href="{websites[0].value}" target="_blank">{websites[0].value.split("//")[1]}</a>' + app
        else:
            websites = CompanyWebsite.objects.filter(company=self).order_by("value")
            if len(websites):
                app = "" if len(websites) == 1 else f" (of {len(websites)} websites)"
                return f'<a href="{websites[0].value}" target="_blank">{websites[0].value.split("//")[1]}</a>' + app
        return "—"

    @cached_property
    def linkedin_url(self):
        urls = CompanyLinkedin.objects.filter(company=self).order_by("value")
        if len(urls):
            app = "" if len(urls) == 1 else f" (of {len(urls)})"
            return f'<a href="{urls[0].value}" target="_blank">{urls[0].value.split("/")[-1]}</a>' + app
        else:
            urls = CompanyLinkedin.objects.filter(company=self).order_by("value")
            if len(urls):
                app = "" if len(urls) == 1 else f" (of {len(urls)})"
                return f'<a href="{urls[0].value}" target="_blank">{urls[0].value.split("/")[-1]}</a>' + app
        return "—"

    class Meta:
        db_table = "company"

class CompanyContact(models.Model):
    company = models.ForeignKey("Company", models.CASCADE)
    street1 = models.CharField(max_length=50, db_column="street1")
    street2 = models.CharField(max_length=50, db_column="street2")
    city = models.CharField(max_length=25, db_column="city")
    state_county = models.CharField(max_length=20, db_column="state_county")
    postalcode = models.CharField(max_length=12, db_column="postalcode")
    country = models.CharField(max_length=50, db_column="country")
    phone_number = models.CharField(max_length=50, db_column="phone_number")
    source = models.CharField(max_length=50, db_column="source")

    class Meta:
        db_table = "company_contact"

class CompanyFirmType(models.Model):
    company = models.ForeignKey("Company", models.CASCADE)
    firm_type = models.ForeignKey("FirmType", models.CASCADE, db_column="firmtype_id")

    class Meta:
        unique_together = (("company", "firm_type"),)
        db_table = "company_firmtype"

class CompanyFormat(models.Model):
    company = models.ForeignKey("Company", models.CASCADE)
    email_format = models.CharField(choices=EmailFormats.choices, max_length=25, db_column="email_format")
    domain = models.CharField(max_length=50, db_column="domain")
    confidence = models.FloatField(default=0, db_column="confidence")

    class Meta:
        unique_together = (("company_id", "email_format", "domain"),)
        db_table = "company_format"

class CompanyLinkedin(models.Model):
    company = models.ForeignKey("Company", models.CASCADE)
    value = models.CharField(max_length=255, unique=True, db_column="value")

    class Meta:
        db_table = "company_linkedin"

class CompanyMappings(models.Model):
    company = models.OneToOneField("Company", models.CASCADE, db_column="company_id")
    preqin_id = models.CharField(max_length=100, default="", blank=True)
    active_campaign_id = models.CharField(max_length=100, default="", blank=True)
    zoho_id = models.CharField(max_length=100, default="", blank=True)

    class Meta:
        db_table = "company_mapping"

class CompanyRelation(models.Model):
    parent = models.ForeignKey("Company", models.CASCADE, related_name="parent", db_column="parent_company_id")
    child = models.ForeignKey("Company", models.CASCADE, related_name="child", db_column="child_company_id")

    class Meta:
        unique_together = (("parent", "child"),)
        db_table = "company_relation"

class CompanyWebsite(models.Model):
    company = models.ForeignKey("Company", models.CASCADE)
    value = models.CharField(max_length=255, db_column="value")
    source = models.CharField(max_length=50, db_column="source")

    class Meta:
        unique_together = (("company", "value", "source"),)
        db_table = "company_website"

class Person(models.Model):
    firstname = models.CharField(max_length=50, db_column="firstname")
    lastname = models.CharField(max_length=50, db_column="lastname")
    date_modified = models.DateTimeField(auto_now=True)
    date_added = models.DateTimeField(auto_now_add=True)

    @cached_property
    def current_role(self):
        c = Career.objects.filter(person=self, date_ended="1970-01-01").order_by("-date_started")
        if len(c):
            app = "" if len(c) == 1 else f" (of {len(c)} current roles)"
            return f"{c[0].role} at {c[0].company}" + app
        return "—"

    @cached_property
    def email_address(self):
        records = [x.value for x in PersonEmail.objects.filter(person=self).order_by("value")]
        if len(records):
            return records
        return ["—"]

    @cached_property
    def linkedin_profile(self):
        linkedin = SocialMedia.objects.get(pk=1)
        return [x.value for x in PersonSocial.objects.filter(person=self, social_media=linkedin).order_by("value")]

    @cached_property
    def phone_number(self):
        records = [x.value for x in PersonPhone.objects.filter(person=self)]

        if len(records):
            return records

        current_companies = [c.company_obj for c in Career.objects.filter(person=self, date_ended="1970-01-01").distinct()]
        numbers = list(set(x.phonenumber for x in CompanyContact.objects.filter(company__in=current_companies)))

        if len(numbers):
            return numbers

        return ["—"]

    class Meta:
        db_table = "person"

class PersonEmail(models.Model):
    person = models.ForeignKey("Person", models.CASCADE)
    value = models.CharField(max_length=255, db_column="value")
    source = models.CharField(max_length=50, db_column="source")
    confidence = models.FloatField(default=0, db_column="confidence")

    class Meta:
        unique_together = (("person", "value", "source"),)
        db_table = "person_email"

class PersonPhone(models.Model):
    person = models.ForeignKey("Person", models.CASCADE)
    value = models.CharField(max_length=255, db_column="value")
    source = models.CharField(max_length=50, db_column="source")

    class Meta:
        unique_together = (("person", "value", "source"),)
        db_table = "person_telephone"

class PersonSocial(models.Model):
    person = models.ForeignKey("Person", models.CASCADE)
    social_media = models.ForeignKey("SocialMedia", models.CASCADE, db_column="social_id")
    value = models.CharField(max_length=255, db_column="value")

    class Meta:
        unique_together = (("person", "value"),)
        db_table = "person_social"

class Career(models.Model):
    person = models.ForeignKey("Person", models.CASCADE)
    company_obj = models.ForeignKey("Company", models.CASCADE, db_column="company_id")
    company = models.CharField(max_length=255, db_column="company", db_index=True)
    role = models.CharField(max_length=100, db_column="role")
    location = models.CharField(max_length=100, db_column="location")
    date_started = models.DateField(db_column="date_started")
    date_ended = models.DateField(db_column="date_ended")
    source = models.CharField(max_length=20, db_column="source")
    date_modified = models.DateTimeField(auto_now=True)
    date_added = models.DateTimeField(auto_now_add=True)

    gfm_move_id = models.CharField(max_length=50, default="", db_column="gfm_move_id")

    def __str__(self):
        return f"{self.person.first_name} {self.person.last_name}, {self.role} at {self.company.name} from {self.date_started} to {self.date_ended}"

    @cached_property
    def start_date_is_known(self):
        if self.date_started == date(1970, 1, 1):
            return False
        return True

    @cached_property
    def end_date_is_known(self):
        if self.date_ended == date(1970, 1, 1):
            return False
        return True

    class Meta:
        unique_together = (("person", "company", "role", "date_started"),)
        db_table = "career"

class CareerJobCategory(models.Model):
    career = models.ForeignKey("Career", models.CASCADE)
    jobcategory = models.ForeignKey("JobCategory", models.CASCADE)

    class Meta:
        unique_together = (("career", "jobcategory"),)
        db_table = "career_jobcategory"
