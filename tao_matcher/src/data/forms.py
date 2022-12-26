import string

from django import forms
from django.core.validators import FileExtensionValidator

from urllib.parse import urlparse

from .fields import AUM_BRACKET, AUM_CATEGORY
from .models import (
    Company, CompanyFormat, CompanyLinkedin, CompanyRelation, CompanyWebsite, FirmType,
    FirmTypes, CompanyFirmType, Person, PersonSocial, PersonEmail, SocialMedia, Specialisms,
    SubTypes, EmailFormats
)

class PersonForm(forms.ModelForm):
    firstname = forms.CharField()
    lastname = forms.CharField()
    linkedin_url = forms.CharField(widget=forms.Textarea)
    email_address = forms.CharField(widget=forms.Textarea, required=False)

    def __init__(self, *args, **kwargs):
        super(PersonForm, self).__init__(*args, **kwargs)

        self.label_suffix = ""

    def save(self, commit=True):
        model_instance = super(PersonForm, self).save(commit=False)
        result = super(PersonForm, self).save(commit=True)

        linkedin_profiles = [x.strip() for x in self.cleaned_data["linkedin_url"].split("\n") if x]
        for profile in linkedin_profiles:
            if "linkedin.com/in/" in profile:
                x = profile.split("/in/")[-1]
                if "/" not in x:
                    obj, created = PersonSocial.objects.get_or_create(
                        person=model_instance,
                        value=profile,
                        social_media=SocialMedia.objects.get(pk=1),
                    )

                    if created:
                        obj.source = "Manual"

                    obj.save()

        removes = PersonSocial.objects.filter(person=model_instance, social_media=SocialMedia.objects.get(pk=1)).exclude(value__in=linkedin_profiles)
        removes.delete()

        email_addresses = [x.strip() for x in self.cleaned_data["email_address"].split("\n") if x]
        for address in email_addresses:
            if len(address.split("@")) == 2:
                x = address.split("@")[-1]
                if "." in x:
                    obj, created = PersonEmail.objects.get_or_create(
                        person=model_instance,
                        value=address,
                    )

                    if created:
                        obj.source = "Manual"
                    obj.save()

        removes = PersonEmail.objects.filter(person=model_instance).exclude(value__in=email_addresses)
        removes.delete()

        return result

    class Meta:
        model = Person
        fields = ("firstname", "lastname")

class PersonUpdateForm(PersonForm):
    def __init__(self, *args, **kwargs):
        super(PersonUpdateForm, self).__init__(*args, **kwargs)
        self.fields["linkedin_url"].initial = "\n".join([x.value for x in PersonSocial.objects.filter(person=self.instance, social_media=SocialMedia.objects.get(name="Linkedin"))])
        self.fields["email_address"].initial = "\n".join([x.value for x in PersonEmail.objects.filter(person=self.instance)])

class CompanyForm(forms.ModelForm):
    name = forms.CharField()
    aum_category = forms.ChoiceField(choices=AUM_CATEGORY, widget=forms.Select)
    aum_bracket = forms.ChoiceField(choices=AUM_BRACKET, widget=forms.Select)
    specialism = forms.ChoiceField(choices=Specialisms.choices, widget=forms.Select)
    email_format = forms.CharField()
    current_parent = forms.CharField(widget=forms.TextInput(attrs={"readonly":"readonly"}))
    parent_company = forms.CharField(widget=forms.Select(attrs={"class": "js-data-example-ajax"}))
    linkedin_url = forms.CharField(widget=forms.Textarea(attrs={"rows": 3, "placeholder": "One link per line..."}))
    websites = forms.CharField(widget=forms.Textarea(attrs={"rows": 3, "placeholder": "One link per line..."}))

    firm_types = forms.MultipleChoiceField(
        choices=FirmTypes.choices,
        widget=forms.SelectMultiple,
    )

    subtypes = forms.MultipleChoiceField(
        choices=SubTypes.choices,
        widget=forms.SelectMultiple,
    )

    def __init__(self, *args, **kwargs):
        super(CompanyForm, self).__init__(*args, **kwargs)

        self.fields["websites"].required = False
        self.fields["firm_types"].required = False
        self.fields["subtypes"].required = False
        self.fields["email_format"].required = False
        self.fields["current_parent"].required = False
        self.fields["parent_company"].required = False

        self.label_suffix = ""

    def clean_linkedin_url(self):
        inputs = self.cleaned_data["linkedin_url"].split("\n")
        inputs = [x.strip() for x in inputs]
        inputs = [x.split("?")[0] for x in inputs]

        for i, _input in enumerate(inputs):
            if not _input.startswith("https://www.linkedin.com/company/"):
                raise forms.ValidationError("Please ensure the Linkedin URL is in the format 'https://www.linkedin.com/company/[company_id]'")

            company_id = _input.split("https://www.linkedin.com/company/")[1].split('/')[0]

            if not len(company_id):
                raise forms.ValidationError("Something went wrong. Please ensure the Linkedin URL is in the format 'https://www.linkedin.com/company/[company_id]'")

            if CompanyLinkedin.objects.filter(value="https://www.linkedin.com/company/"+company_id).exclude(company=self.instance).exists():
                raise forms.ValidationError(f"One of the inputs ({company_id}) is currently associated with another company. Please merge it with this entry to proceed.")

            inputs[i] = company_id

        inputs = list(set(inputs))

        return ["https://www.linkedin.com/company/" + company_id for company_id in inputs]

    def clean_websites(self):
        inputs = self.cleaned_data["websites"].split("\n")
        inputs = [x.strip() for x in inputs]
        inputs = [x.split("?")[0] for x in inputs]

        for i, website in enumerate(inputs):
            inputs[i] = urlparse(website).netloc

            if website and not inputs[i]:
                raise forms.ValidationError("Please note that all URLs must be in the format 'http[s]://[website]'")

        inputs = list(set(inputs))
        try:
            inputs.remove("")
        except ValueError:
            pass

        return ["https://" + i for i in inputs]

    def clean_parent_company(self):
        if self.cleaned_data["parent_company"] == self.cleaned_data["name"]:
            raise forms.ValidationError("A company cannot be marked as it's own parent")
        if self.cleaned_data["parent_company"]:
            if CompanyRelation.objects.filter(parent=self.instance, child=Company.objects.get(pk=self.cleaned_data["parent_company"])).exists():
                raise forms.ValidationError("A company cannot be marked as a parent of it's own parent")

        return self.cleaned_data["parent_company"]

    def clean_email_format(self):
        _input = self.cleaned_data["email_format"].strip().lower().replace(" ", "")

        if not _input:
            return _input

        allowed_chars = string.ascii_letters + string.digits + "@.-_"

        if any(c not in allowed_chars for c in _input):
            raise forms.ValidationError("One or more inputs contains invalid characters.")
        if "@" not in _input:
            raise forms.ValidationError("One or more inputs not a valid email format.")

        _format, _ = _input.split("@")

        if not any(email_format[0] == _format for email_format in EmailFormats.choices):
            raise forms.ValidationError("This is not a valid option for email format.")

        return _input

    def save(self, commit=True):
        model_instance = super(CompanyForm, self).save(commit=False)
        result = super(CompanyForm, self).save(commit=True)

        if self.cleaned_data["parent_company"]:
            company = Company.objects.get(pk=self.cleaned_data["parent_company"])

            obj, created = CompanyRelation.objects.get_or_create(
                parent=company,
                child=model_instance,
            )
            if created:
                obj.save()

        CompanyLinkedin.objects.filter(company=model_instance).delete()
        _input = self.cleaned_data["linkedin_url"]

        for website in _input:
            if website:
                x, created = CompanyLinkedin.objects.get_or_create(
                    pk=CompanyLinkedin.objects.last().pk+1 if CompanyLinkedin.objects.last() else 1,
                    company=model_instance,
                    value=website,
                )
                x.save()

        CompanyWebsite.objects.filter(company=model_instance).delete()
        if self.cleaned_data["websites"]:
            _input = self.cleaned_data["websites"]

            for website in _input:
                if website:
                    x, created = CompanyWebsite.objects.get_or_create(
                        pk=CompanyWebsite.objects.last().pk+1 if CompanyWebsite.objects.last() else 1,
                        company=model_instance,
                        value=website,
                    )
                    if created:
                        x.source = "Manual"
                    x.save()

        if self.cleaned_data["email_format"]:
            _input = self.cleaned_data["email_format"]
            _format, domain = _input.split("@")

            CompanyFormat.objects.filter(company=model_instance).delete()

            for email_format in EmailFormats.choices:
                if email_format[0] == _format:
                    x, _ = CompanyFormat.objects.get_or_create(
                        pk=CompanyFormat.objects.last().pk+1 if CompanyFormat.objects.last() else 1,
                        company=model_instance,
                        email_format=_format,
                        domain=domain,
                        confidence=1.0
                    )
                    x.save()
        return result

    class Meta:
        model = Company
        fields = ("name", "linkedin_url", "current_parent", "parent_company", "email_format", "websites", "aum_category", "aum_bracket", "specialism")

class CompanyUpdateForm(CompanyForm):
    def __init__(self, *args, **kwargs):
        super(CompanyUpdateForm, self).__init__(*args, **kwargs)

        try:
            self.fields["linkedin_url"].initial = "\n".join([
                web.value for web in CompanyLinkedin.objects.filter(company=self.instance)
            ])
        except:
            self.fields["linkedin_url"].initial = ""

        try:
            self.fields["websites"].initial = "\n".join([
                web.value for web in CompanyWebsite.objects.filter(company=self.instance)
            ])
        except:
            self.fields["websites"].initial = ""

        self.fields["firm_types"].initial = [
            ft.firm_type for ft in FirmType.objects.filter(companyfirmtype__company=self.instance, is_subtype=0)
        ]
        self.fields["subtypes"].initial = [
            ft.firm_type for ft in FirmType.objects.filter(companyfirmtype__company=self.instance, is_subtype=1)
        ]

        try:
            x = CompanyFormat.objects.get(company=self.instance)
            self.fields["email_format"].initial = f'{x.email_format}@{x.domain}'
        except:
            self.fields["email_format"].initial = ''

        try:
            self.fields["current_parent"].initial = CompanyRelation.objects.get(child=self.instance).parent.name
        except:
            self.fields["current_parent"].initial = ""

    def save(self, commit=True):
        model_instance = super(CompanyUpdateForm, self).save(commit=False)
        result = super(CompanyUpdateForm, self).save(commit=True)

        if self.cleaned_data["name"]:
            model_instance.name = self.cleaned_data["name"]

        company_firm_types = CompanyFirmType.objects.filter(company=model_instance)

        keeps = []
        for t in self.cleaned_data["firm_types"] + self.cleaned_data["subtypes"]:
            obj, _ = CompanyFirmType.objects.get_or_create(
                company=model_instance,
                firm_type=FirmType.objects.get(firm_type=t),
            )
            keeps.append(obj)

        for t in company_firm_types:
            if t not in keeps:
                t.delete()

        model_instance.aum_category = int(self.cleaned_data["aum_category"])
        model_instance.save()
        return result

class RoleSearchForm(forms.Form):
    companies = forms.CharField(widget=forms.Textarea(attrs={"placeholder": "One company per line..."}))
    roles = forms.CharField(widget=forms.Textarea(attrs={"placeholder": "One role per line..."}))

    def __init__(self, *args, **kwargs):
        super(RoleSearchForm, self).__init__(*args, **kwargs)
        self.fields["companies"].required = False
        self.fields["roles"].required = False

class UploadForm(forms.Form):
    files = forms.FileField(
        label="",
        allow_empty_file=False,
        validators=[FileExtensionValidator(allowed_extensions=["csv", "json"])],
        widget=forms.FileInput(attrs={"multiple": True}),
    )

    def clean_files(self):
        limit = 20
        files = self.files.getlist("files")
        if len(files) > limit:
            raise forms.ValidationError(f"Please upload a maximum of {limit} files.")

        for file in files:
            if file.size > 500000000:
                raise forms.ValidationError(f"File exceeds maximum size limit. Please keep below 500MB.")

        return files

class ScrapingForm(forms.Form):
    inputs = forms.CharField(widget=forms.Textarea(attrs={"placeholder": "One link per line..."}), required=True)

    def clean_inputs(self):
        inputs = [x.strip() for x in self.cleaned_data["inputs"].split("\n")]
        inputs = [x.split("?")[0] for x in inputs if x]

        if any("linkedin.com" not in i for i in inputs):
            raise forms.ValidationError(f"One or more links invalid. Please ensure all links contain linkedin.com")

        inputs = [x.split("linkedin.com")[1] for x in inputs]

        if any("/in/" not in i and "/company/" not in i for i in inputs):
            raise forms.ValidationError(f"Please only supply personal or company links i.e. /in or /company")

        allowed_chars = string.ascii_letters + string.digits + "-%"

        inputs_id = {
            "people": [],
            "companies": [],
        }

        for i in inputs:
            if "/in/" in i:
                inputs_id["people"].append(i.split("/in/")[1])
            elif "/company/" in i:
                inputs_id["companies"].append(i.split("/company/")[1])

        if sum(len(inputs_id[i]) for i in inputs_id) != len(inputs):
            raise forms.ValidationError("Error processing links. Please try again")

        if any(c not in allowed_chars for i in inputs_id for c in i):
            raise forms.ValidationError("One or more links contain invalid characters")

        inputs = [
            f"https://www.linkedin.com/in/{i}" for i in inputs_id["people"]
        ] + [
            f"https://www.linkedin.com/company/{i}" for i in inputs_id["companies"]
        ]

        people_links = PersonSocial.objects.filter(social_media__pk=1).values_list("value", flat=True)
        people_links = list(people_links)

        company_links = Company.objects.filter(pk__gt=0).values_list("linkedin_url", flat=True)
        company_links = list(company_links)

        return [i for i in inputs if i not in people_links + company_links]

class CompanyMergeForm(forms.Form):
    company_id = forms.ChoiceField(widget=forms.Select(attrs={"class": "js-data-example-ajax"}))

class ReportSearchForm(forms.Form):
    company_name = forms.ChoiceField(widget=forms.Select(attrs={"class": "js-data-example-ajax"}))
