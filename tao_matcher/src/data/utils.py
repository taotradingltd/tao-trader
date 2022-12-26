import datetime
import json
import os

import dateutil.parser
import numpy as np
import pandas as pd

from django.conf import settings
from django.utils import timezone

from gfm_utils.text import preprocessing
from gfm_utils.text.classification import taxonomy

from numpy import NaN

from typing import Dict, List

from unidecode import unidecode
from urllib.parse import unquote

from .models import (
    Career,
    CareerJobCategory,
    Company,
    CompanyLinkedin,
    JobCategory,
    Person,
    PersonSocial,
    SocialMedia
)

UPLOAD_RUNNING = False

def csvs_to_db(csvs: Dict[str, pd.DataFrame]):
    assert(all(type(csv) is pd.DataFrame for key in csvs for csv in csvs[key].values()))

    for key in csvs:
        for key2 in csvs[key]:
            to_upload = None
            csv = csvs[key][key2]

            settings.LOGGER.info(f"{key} - {key2} - {csv.shape}")

            potential = {
                "person": ["person_id", "firstname", "lastname", "linkedin_profile"],
                "company": ["company_id", "company_url"],
                "career": ["career_id", "person_id", "company_id", "company_name", "role", "location", "date_started", "date_ended", "source"],
                "careerjobcategory": ["career_id", "jobcategory_id"],
            }

            # find which table we want to upload
            for table in potential:
                if all(x in potential[table] for x in csv.columns):
                    to_upload = table
                    break

            if not to_upload:
                settings.LOGGER.info(csv.columns)
                settings.LOGGER.error("This file was not valid. Please upload a valid file.")
                continue
            settings.LOGGER.info(to_upload)

            if to_upload == "person":
                if key2 == "insertions":
                    settings.LOGGER.info("Building objects (person)...")
                    objs = [
                        Person(
                            pk=row["person_id"],
                            firstname=row["firstname"],
                            lastname=row["lastname"],
                        )
                    for _, row in csv.iterrows()]
                    settings.LOGGER.info("Creating objects...")
                    Person.objects.bulk_create(objs)

                    settings.LOGGER.info("Building objects (social)...")
                    objs_social = [
                        PersonSocial(
                            person=Person.objects.get(pk=row["person_id"]),
                            value=row["linkedin_profile"],
                            social_media=SocialMedia.objects.get(pk=1),
                        )
                    for _, row in csv.iterrows()]

                    settings.LOGGER.info("Creating objects...")
                    PersonSocial.objects.bulk_create(objs_social)

                else:
                    settings.LOGGER.info("Getting objects...")
                    objs = [
                        Person(
                            pk=row["person_id"],
                        )
                    for _, row in csv.iterrows()]

                    for obj in objs: obj.date_modified = datetime.datetime.now(tz=timezone.utc)
                    settings.LOGGER.info("Updating objects...")
                    Person.objects.bulk_update(objs, ["date_modified"])

            elif to_upload == "career":
                if key2 == "insertions":
                    settings.LOGGER.info("Building objects...")
                    objs = []

                    for _, row in csv.iterrows():
                        try:
                            objs.append(
                                Career(
                                    pk=row["career_id"],
                                    person=Person.objects.get(pk=row["person_id"]),
                                    company_obj=Company.objects.get(pk=row["company_id"]),
                                    company=row["company_name"],
                                    role=row["role"],
                                    location=row["location"],
                                    date_started=row["date_started"],
                                    date_ended=row["date_ended"],
                                    source="Linkedin"
                                )
                            )
                        except:
                            continue

                    settings.LOGGER.info("Creating objects...")

                    for obj in objs:
                        try:
                            obj.save()
                        except:
                            continue

                else:
                    settings.LOGGER.info("Getting objects...")
                    objs = []
                    for _, row in csv.iterrows():
                        obj = Career.objects.get(pk=row["career_id"])

                        obj.location = row["location"]
                        obj.date_started = row["date_started"]
                        obj.date_ended = row["date_ended"]

                        objs.append(obj)

                    settings.LOGGER.info("Updating objects...")
                    Career.objects.bulk_update(objs, ["location", "date_started", "date_ended"])

            elif to_upload == "company":
                if key2 == "insertions":
                    settings.LOGGER.info("Building objects...")
                    objs = []
                    li_objs = []
                    for _, row in csv.iterrows():
                        company = Company(
                            pk=row["company_id"],
                        )
                        company_linkedin = CompanyLinkedin(
                            company=company,
                            value=row["company_url"]
                        )
                        objs.append(company)
                        li_objs.append(company_linkedin)
                    settings.LOGGER.info("Creating objects...")
                    Company.objects.bulk_create(objs)
                    CompanyLinkedin.objects.bulk_create(li_objs)

                else:
                    settings.LOGGER.info("Building new CompanyLinkedin objects...")
                    for _, row in csv.iterrows():
                        li_obj, _ = CompanyLinkedin.objects.get_or_create(
                            company=Company.objects.get(pk=row["company_id"]),
                            value=row["company_url"]
                        )
                        li_obj.save()

            elif to_upload == "careerjobcategory":
                settings.LOGGER.info("Building objects...")
                objs = []

                for _, row in csv.iterrows():
                    try:
                        objs.append(
                            CareerJobCategory(
                                career=Career.objects.get(pk=row["career_id"]),
                                jobcategory=JobCategory.objects.get(pk=row["jobcategory_id"]),
                            )
                        )
                    except:
                        continue

                settings.LOGGER.info("Creating objects...")

                for obj in objs:
                    try:
                        obj.save()
                    except:
                        continue

    settings.LOGGER.info("Uploaded all data...")

def profile_unquote(url: str) -> str:
    orig = url
    count = 0

    while "%" in url and count <= 100:
        url = unquote(url, encoding="utf-8-sig").lower()

        if count == 100:
            settings.LOGGER.warning(orig)
        count += 1

    return url

def json_to_csv(people: dict, kind: str) -> pd.DataFrame:
    methods = {
        "iscraper": _iscraper_json_to_csv,
        "brightdata": _brightdata_json_to_csv,
    }
    return methods[kind](people)

def _brightdata_json_to_csv(people: dict) -> pd.DataFrame:
    brightdata_fields = ["url", "name", "experience"]

    # Drop erroneous scrapes
    people = [p for p in people if not p.get("error")]

    profiles = []
    for p in people:
        p_new = {}
        for key in brightdata_fields:
            if key in p:
                p_new[key] = p[key]
        profiles.append(p_new)

    df = pd.DataFrame(profiles)

    df = df[df["url"].notna()][[
        "url", "name", "experience"
    ]]

    df["profile_id"] = df["url"].apply(lambda x: x.split("?")[0].split("/")[-1])

    df = df[
        ["profile_id", "name", "experience"]
    ].explode("experience").reset_index(drop=True)

    df = pd.concat([df.drop(["experience"], axis=1), pd.json_normalize(df["experience"])], axis=1)

    # Expand multiple role companies
    df = df.explode("positions").reset_index(drop=True)
    df = pd.concat([df.drop(["positions"], axis=1), pd.json_normalize(df["positions"])], axis=1)

    df = df.groupby(level=0, axis=1).apply(lambda x: x.apply(preprocessing.sjoin, axis=1))
    df = df.replace({"": NaN})

    df = df[df["company"].notna() & df["title"].notna()].fillna("")

    df.rename(columns={
        "profile_id": "profile_id",
        "name": "name",
        "company": "company_name",
        "subtitleURL": "company_url",
        "title": "role",
        "location": "location",
        "start_date": "date_started",
        "end_date": "date_ended",
    }, inplace=True)

    return clean_dataframe(df)

def _iscraper_json_to_csv(people: dict) -> pd.DataFrame:
    profiles = []
    for person in people:
        profile_processed = {
            "name": person["first_name"] + " " + person["last_name"],
            "profile_id": person["profile_id"],
        }

        experience = []

        for position in person["position_groups"]:
            role_dict = {
                "company_name": position["company"]["name"],
                "company_url": position["company"]["url"],
            }

            for role in position["profile_positions"]:
                role_dict_copy = role_dict.copy()
                role_dict_copy["role"] = role["title"]
                role_dict_copy["location"] = role["location"]

                start_date = None
                end_date = None

                if role.get("date"):
                    if role.get("date").get("start"):
                        if role.get("date").get("start").get("year"):
                            start_date = str(
                                role.get("date").get("start").get("year")
                            )
                            if role.get("date").get("start").get("month"):
                                start_date = start_date +\
                                "-" +\
                                str(role.get("date").get("start").get("month")) +\
                                "-01"
                            else:
                                start_date += "-01-01"

                    if role.get("date").get("end"):
                        if role.get("date").get("end").get("year"):
                            end_date = str(
                                role.get("date").get("end").get("year")
                            )
                            if role.get("date").get("end").get("month"):
                                end_date = end_date +\
                                "-" +\
                                str(role.get("date").get("end").get("month")) +\
                                "-01"
                            else:
                                end_date += "-01-01"

                if start_date is None:
                    start_date = "1970-01-01"
                if end_date is None:
                    end_date = "1970-01-01"

                role_dict_copy["date_started"] = start_date
                role_dict_copy["date_ended"] = end_date

                # If no location and role is "current", use profile location
                if role_dict_copy.get("date_started") != "1970-01-01" and\
                    role_dict_copy.get("date_ended") == "1970-01-01" and\
                        not role_dict_copy["location"]:
                    role_dict_copy["location"] = person.get("location").get("default")
                experience.append(role_dict_copy)

        profile_processed["experience"] = experience
        profiles.append(profile_processed)

    del people

    df = pd.DataFrame(profiles)
    df = df.explode("experience")
    df.reset_index(drop=True, inplace=True)
    df = pd.concat(
        [
            df.drop(["experience"], axis=1), pd.json_normalize(df["experience"])
        ], axis=1
    )

    return clean_dataframe(df)

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean a given DataFrame of employee career histories.

    Args:
        df: input DataFrame should have the following **exact** columns:
            "name": the full name of the person; no need to clean in advance
                i.e. Ben Manson, Cfa will automaticall become Ben Manson
            "profile_id": the Linkedin id of the person
                i.e. linkedin.com/in/-ben-manson?q=some+query+string
                    -> -ben-manson
            "company_name": the name of the company
            "company_url": the linkedin_url of the company; will be cleaned
                in this function
            "role": role or NaN or ""
            "location": location or NaN or ""
            "date_started": date_started in a parsable string format for
                dateutil.parser
            "date_ended": date_ended in a parsable string format for
                dateutil.parser

    Returns:
        df: the same DataFrame, but cleaned
    """

    block_list = [
        "jon-alexander-morris-47845522b",
    ]

    df = df[~df["profile_id"].isin(block_list)]

    def parse_date(x):
        try:
            x = dateutil.parser.parse(x)
        except:
            x = dateutil.parser.parse("1970-01-01")

        if x.date() < datetime.date(1970, 1, 1):
            x = dateutil.parser.parse("1970-01-01")
        return x

    df["date_started"].fillna("1970-01-01", inplace=True)
    df["date_ended"].fillna("1970-01-01", inplace=True)
    df["date_started"] = df["date_started"].apply(parse_date)
    df["date_ended"] = df["date_ended"].apply(parse_date)

    df["date_started"] = df["date_started"].dt.date
    df["date_ended"] = df["date_ended"].dt.date

    df.fillna("", inplace=True)

    def get_company_url(value):
        value = value.strip()
        if not len(value):
            return ""
        value = value[:-1] if value[-1] == "/" else value
        value = "https://www.linkedin.com/company/" + value.split("/")[-1].split("?")[0].lower()
        return value

    df["company_url"] = df["company_url"].apply(get_company_url)

    df["profile_id"] = "https://www.linkedin.com/in/" + df["profile_id"]
    df["profile_id"] = df["profile_id"].apply(lambda x: x.split("?")[0].lower())

    df.rename(columns={"profile_id": "linkedin_profile"}, inplace=True)

    str_cols = [
        "name",
        "linkedin_profile",
        "company_name",
        "company_url",
        "role",
        "location",
    ]

    url_cols = [
        "linkedin_profile",
        "company_url",
    ]

    for col in str_cols:
        df[col] = df[col].apply(str.strip)

    for col in url_cols:
        df[col] = df[col].apply(profile_unquote)

    df = df[df["company_name"] != ""]
    df.reset_index(drop=True, inplace=True)

    return df.fillna("")

def get_db_changes(csv: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    dfs = {
        "person": {
            "insertions": None,
            "updates": None,
        },
        "company": {
            "insertions": None,
            "updates": None,
        },
        "career": {
            "insertions": None,
            "updates": None,
        },
        "career_jobcategory": {
            "insertions": None,
        }
    }

    # ====================================
    #               PERSON
    # ====================================

    # To split the available people in the provided CSV into insertions and updates, we need to get the unique
    # people within the CSV via linkedin_profile links. Updates are the people within this list who exist
    # in our database already - we must update them so that we know we have scraped them. Insertions are the
    # rest of the people.

    settings.LOGGER.debug("Getting people from uploaded csv and database.")

    people = csv[["name", "linkedin_profile"]].drop_duplicates("linkedin_profile")
    in_db_people = pd.DataFrame(
        list(Person.objects.all().values("pk", "firstname", "lastname", "personsocial__value")),
        columns=["pk", "firstname", "lastname", "personsocial__value"],
    ).drop_duplicates("personsocial__value")

    settings.LOGGER.debug(f"{in_db_people.shape[0]} people currently in the database.")

    insertions = people[~people.linkedin_profile.isin(in_db_people["personsocial__value"])].reset_index(drop=True)
    updates = people[people.linkedin_profile.isin(in_db_people["personsocial__value"])].reset_index(drop=True)

    # Get existing ids
    settings.LOGGER.debug("Merging updates with db to get person_ids.")
    updates = pd.merge(
        updates,
        in_db_people,
        "left",
        left_on="linkedin_profile",
        right_on="personsocial__value",
    )

    # Fix names
    settings.LOGGER.debug("Formatting insertions names.")
    temp = insertions["name"].apply(lambda x: preprocessing.clean_name(x, 'person', False, False))
    insertions["firstname"] = temp.apply(lambda x: x[0])
    insertions["lastname"] = temp.apply(lambda x: x[1])
    insertions.drop(columns=["name"], inplace=True)

    # Get new person_ids
    settings.LOGGER.debug("Assigning insertions person_ids.")
    insertions = insertions.reset_index().rename(columns={"index": "person_id"})

    try:
        max_person_id = Person.objects.last().pk + 1
    except AttributeError:
        max_person_id = 1

    insertions["person_id"] += max_person_id
    settings.LOGGER.debug("Assigned insertions person_ids.")

    cols = [
        "person_id",
        "firstname",
        "lastname",
        "linkedin_profile"
    ]

    settings.LOGGER.debug("Removing unused columns.")
    insertions = insertions[cols]
    updates = updates.rename(columns={"pk": "person_id"})[cols]
    settings.LOGGER.debug("Removing unused columns.")

    settings.LOGGER.debug(insertions.head())
    settings.LOGGER.debug(updates.head())

    dfs["person"]["insertions"] = insertions[["person_id", "firstname", "lastname", "linkedin_profile"]]
    dfs["person"]["updates"] = updates[["person_id", "firstname", "lastname", "linkedin_profile"]]

    del people
    del in_db_people

    # ====================================
    #               COMPANY
    # ====================================

    settings.LOGGER.debug("Getting companies from csv and database.")
    companies = csv[["company_name", "company_url"]].drop_duplicates(subset=["company_name", "company_url"])
    in_db_companies = pd.merge(
        pd.DataFrame(
            list(Company.objects.all().values("pk", "cleaned_name")),
            columns=["pk", "cleaned_name"],
        ),
        pd.DataFrame(
            list(CompanyLinkedin.objects.all().values("company__pk", "value")),
            columns=["company__pk", "value"]
        ).rename(columns={"company__pk": "pk", "value": "linkedin_url"}),
        "left",
        "pk"
    )
    in_db_companies["decoded"] = in_db_companies["linkedin_url"].fillna("").apply(unidecode)

    companies = companies[~companies["company_url"].isin(in_db_companies["linkedin_url"])]

    settings.LOGGER.debug(f"{in_db_companies.shape[0]} companies currently in the database.")

    companies = companies.dropna(subset=["company_url"])
    companies["cleaned_name"] = companies["company_name"].fillna("").apply(lambda x: preprocessing.clean_name(x, 'company', True, False))

    settings.LOGGER.debug("Filtering companies who's Linkedin wasn't in db (insertions).")
    insertions = companies[~companies["company_url"].isin(in_db_companies["linkedin_url"])].drop_duplicates("company_url").reset_index(drop=True)
    insertions = insertions[["company_url", "cleaned_name"]]
    insertions = insertions[(insertions["company_url"] != "") & (insertions["company_url"].notna())]

    # Quick fix to get past accents being treated the same by the MySQL engine
    insertions["decoded"] = insertions["company_url"].apply(unidecode)
    insertions = insertions.drop_duplicates("decoded")
    insertions = insertions[~insertions["decoded"].isin(in_db_companies["decoded"])]

    # Build the updates table

    # This table is formed by merging the database entries without a linkedinurl with the insertions i.e. new
    # companies on cleaned name.

    settings.LOGGER.debug("Finding updated companies i.e. new Linkedin urls.")
    potential_updates = in_db_companies[(in_db_companies["linkedin_url"] == "") & (in_db_companies["cleaned_name"] != "")]
    potential_updates = potential_updates[["pk", "linkedin_url", "cleaned_name"]]

    # TODO: should we only look at insertions that have a linkedin url in this step???
    updates = pd.merge(
        potential_updates,
        insertions[(insertions["cleaned_name"].notna()) & (insertions["company_url"].notna())],
        "inner",
        "cleaned_name",
    ).drop_duplicates(subset=["pk"])[["pk", "company_url"]].rename(columns={"pk": "company_id"})
    settings.LOGGER.debug(f"{updates.shape[0]} new urls found for existing companies.")

    # Remove insertions which are now moved into updates
    insertions = insertions[~insertions["company_url"].isin(updates["company_url"])]

    # Add in new company ids to insertions table
    settings.LOGGER.debug("Calculating new company_ids for insertions.")
    insertions.reset_index(inplace=True, drop=True)
    insertions = insertions.reset_index().rename(columns={"index": "company_id"})

    try:
        max_company_id = Company.objects.last().pk + 1
        if max_company_id == 0:
            max_company_id += 1
    except AttributeError:
        max_company_id = 1

    insertions["company_id"] += max_company_id

    settings.LOGGER.debug(insertions.head())
    settings.LOGGER.debug(updates.head())

    dfs["company"]["insertions"] = insertions[["company_id", "company_url"]]
    dfs["company"]["updates"] = updates[["company_id", "company_url"]]

    del companies

    # ====================================
    #               CAREER
    # ====================================

    career = csv.copy(deep=True)

    people = pd.concat([dfs["person"]["insertions"], dfs["person"]["updates"]]).drop_duplicates("linkedin_profile")
    career["cleaned_name"] = career["company_name"].apply(lambda x: preprocessing.clean_name(x, 'company', True, False))

    # Add in person_ids
    career = pd.merge(
        career,
        people[["person_id", "linkedin_profile"]],
        "left",
        "linkedin_profile",
    )

    first = pd.merge(
        in_db_companies.rename(columns={"pk": "company_id", "linkedin_url": "company_url"}),
        dfs["company"]["updates"].rename(columns={"company_url": "new_url"}),
        "left",
        "company_id"
    ).replace({"": np.NaN})

    first["company_url"].fillna(first["new_url"], inplace=True)
    first.drop(columns="new_url", inplace=True)
    first.fillna("", inplace=True)

    company = pd.concat([
        first,
        dfs["company"]["insertions"],
    ]).drop_duplicates(subset=["company_id"])

    company_url_df = company.replace({"": np.NaN}).dropna(subset=["company_url"])[["company_url", "company_id"]]
    company_url_dict = dict(zip(company_url_df["company_url"], company_url_df["company_id"]))
    cleaned_name_df = company.replace({"": np.NaN}).dropna(subset=["cleaned_name"]).drop_duplicates(subset=["cleaned_name"])[["cleaned_name", "company_id"]]
    cleaned_name_dict = dict(zip(cleaned_name_df["cleaned_name"], cleaned_name_df["company_id"]))

    career["company_url"].fillna("", inplace=True)
    career["cleaned_name"].fillna("", inplace=True)

    # Add company_ids to career
    company_ids = []

    for _, row in career.iterrows():
        company_url = row["company_url"]
        cleaned_name = row["cleaned_name"]

        # If URL exists
        if company_url:
            try:
                company_id = company_url_dict[company_url]  # set company_id
            except KeyError:
                try:
                    company_id = company_url_dict[unidecode(company_url)]  # set company_id
                except KeyError:
                    company_id = -1

        # Otherwise match to cleaned_name
        elif cleaned_name:
            try:
                company_id = cleaned_name_dict[cleaned_name]
            except:
                company_id = -1

        else:
            company_id = -1

        company_ids.append(company_id)

    career["company_id"] = company_ids

    career = career[["person_id", "company_id", "company_name", "role", "location", "date_started", "date_ended"]]

    people_ids = dfs["person"]["insertions"]["person_id"].to_list()
    people_ids += dfs["person"]["updates"]["person_id"].to_list()
    people_ids = list(set(people_ids))

    objs = Career.objects.filter(person__pk__in=people_ids)
    objs = list(objs.values(
            "pk",
            "person__pk",
            "company_obj__pk",
            "company",
            "role",
            "location",
            "date_started",
            "date_ended"
        )
    )

    settings.LOGGER.debug("Converting to dataframe.")
    in_db_career = pd.DataFrame(
        objs,
        columns=[
            "pk", "person__pk", "company_obj__pk",
            "company", "role", "location",
            "date_started", "date_ended"
        ]
    )

    in_db_career.rename(
        columns={
            "pk": "career_id",
            "person__pk": "person_id",
            "company_obj__pk": "company_id",
            "company": "company_name",
        }, inplace=True)

    in_db_career = in_db_career[["career_id", "person_id", "company_id", "company_name", "role", "location", "date_started", "date_ended"]]

    career["source"] = "new"
    career["career_id"] = None

    in_db_career["source"] = "current"

    for df in [career, in_db_career]:
        # Convert to datetime objects and set "wrong" dates to NaT objects
        for date_col in ["date_started", "date_ended"]:
            df[date_col] = pd.to_datetime(df[date_col], format="%Y-%m-%d", errors="coerce")

        df.dropna(subset=["date_started", "date_ended"], inplace=True)

    # Merge new and current data
    df = pd.concat([career, in_db_career])

    # Drop rows where no changes have taken place (on relevant columns)
    delta = df.drop_duplicates(
        subset=[
            "person_id",
            "company_name",
            "role",
            "date_started",
            "date_ended",
        ],
        keep=False
    ).reset_index(drop=True)

    empty = delta[
        (delta["role"] == "")
    ].index.tolist()

    delta.drop(empty, inplace=True)
    ### ONE TIME CHANGE - ONLY TO FIX DATES
    delta["year_started"] = delta["date_started"].dt.year
    delta["year_ended"] = delta["date_ended"].dt.year

    delta["month_started"] = delta["date_started"].dt.month
    delta["month_ended"] = delta["date_ended"].dt.month

    # Month is different but is actually the same entry
    to_drop = delta[
        (delta.duplicated(subset=["person_id", "company_name", "role", "year_started", "year_ended"], keep=False))
        & (~delta.duplicated(subset=["person_id", "company_name", "role", "month_started", "month_ended"], keep=False))
    ].index.tolist()
    delta.drop(to_drop, inplace=True)

    # Columns to group duplicates by
    dupe_cols = ["person_id", "company_name", "role", "year_started"]

    # Sorted DataFrame of changes
    changes = delta[
        delta.duplicated(
            subset=dupe_cols,
            keep=False
        )
    ].sort_values(dupe_cols)

    # Create IDs representing the changes
    changes["change_id"] = changes.groupby(dupe_cols, sort=False).ngroup() + 1

    temp = changes["change_id"].value_counts()

    # Only keep pairs
    changes = changes[changes["change_id"].isin(temp[temp == 2].index.tolist())]

    # Get new roles
    insertions = delta[
        (~delta.duplicated(
            subset=dupe_cols,
            keep=False
        ))
        & (delta["source"] == "new")
    ].sort_values("date_started", ascending=False)

    updates = []

    for change_id in changes["change_id"].unique():
        change = changes[changes["change_id"] == change_id].reset_index(drop=True)
        change.loc[1, "date_ended"] = change.loc[0]["date_ended"]

        # If current location/company_url is empty
        for col in ["location"]:
            if not change.loc[1][col]:
                change.loc[1, col] = change.loc[0][col]

        updates.append(change.loc[1])

    updates = pd.DataFrame(updates)

    if len(updates):
        updates.dropna(subset=["career_id"], inplace=True)
        updates.dropna(subset=["person_id"], inplace=True)

        for col in ["person_id", "career_id"]:
            updates[col] = updates[col].astype(int)

    # Add in new company ids to insertions table
    insertions = pd.concat([insertions, in_db_career], ignore_index=True)[
        ["career_id", "person_id", "company_id", "company_name", "role", "location", "date_started", "date_ended", "source"]
    ]

    insertions.drop_duplicates(
        subset=[
            "person_id",
            "company_name",
            "role",
            "date_started",
        ],
        keep=False,
        inplace=True
    )

    insertions = insertions[insertions["source"] != "current"]

    insertions.drop(columns=["career_id", "source"], inplace=True)
    insertions.reset_index(inplace=True, drop=True)
    insertions = insertions.reset_index().rename(columns={"index": "career_id"})

    try:
        max_career_id = Career.objects.last().pk
    except AttributeError:
        max_career_id = 0

    next_id = max_career_id + 1

    insertions["career_id"] += next_id

    settings.LOGGER.debug(insertions.head())
    settings.LOGGER.debug(updates.head())

    dfs["career"]["insertions"] = insertions[
        ["career_id", "person_id", "company_id", "company_name", "role", "location", "date_started", "date_ended"]
    ]

    dfs["career"]["updates"] = updates[
        ["career_id", "person_id", "company_id", "company_name", "role", "location", "date_started", "date_ended"]
    ]

    # ====================================
    #         CAREER_JOBCATEGORY
    # ====================================

    categories = pd.DataFrame(
        list(JobCategory.objects.all().values("pk", "category")),
        columns=["pk", "category"]
    )
    categories = dict(zip(categories["category"], categories["pk"]))

    jobcategories = dfs["career"]["insertions"][["career_id", "role"]].copy(deep=True)
    jobcategories["jobcategory_id"] = jobcategories["role"].fillna("").apply(taxonomy.get_job_category)

    # career_job_category insertions
    jobcategories.drop(columns=["role"], inplace=True)
    jobcategories.dropna(subset=["jobcategory_id"], inplace=True)
    jobcategories["jobcategory_id"] = jobcategories["jobcategory_id"].str.split("\|\|")
    jobcategories = jobcategories.explode(column="jobcategory_id")

    jobcategories["jobcategory_id"] = jobcategories["jobcategory_id"].replace(categories)

    for col in jobcategories.columns:
        jobcategories[col] = jobcategories[col].astype(int)

    dfs["career_jobcategory"]["insertions"] = jobcategories

    return dfs

def json_to_db(paths: List[str]):
    global UPLOAD_RUNNING

    if UPLOAD_RUNNING:
        settings.LOGGER.warning("Process already running. Please try again later.")
        return

    UPLOAD_RUNNING = True

    try:
        for path in paths:
            settings.LOGGER.info(f"Loading {path} as json")
            with open(f"{path}", "r") as f:
                people = json.load(f)

            if all("profile_id" in person for person in people):
                kind = "iscraper"
            elif all("input" in person for person in people):
                kind = "brightdata"
            else:
                continue

            settings.LOGGER.info(f"{path} is kind {kind}")
            settings.LOGGER.info(f"{len(people)} people loaded; converting to csv.")

            # Turn our profile json file into a csv
            csv = json_to_csv(people, kind=kind)
            settings.LOGGER.info(f"csv of shape {csv.shape} created.")

            # Get the tables to update in the db
            settings.LOGGER.info("Getting updates for the database.")
            db_updates = get_db_changes(csv)
            settings.LOGGER.info("Adding updates to the database.")
            csvs_to_db(db_updates)
    finally:
        for path in paths:
            os.remove(path)
        UPLOAD_RUNNING = False
