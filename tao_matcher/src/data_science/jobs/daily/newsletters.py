import email
import os
import re
import requests

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.dates as mdates
import seaborn as sns


from django.conf import settings
from django_extensions.management.jobs import DailyJob

URL_v1 = "https://globalfundmedia13726.api-us1.com/admin/api.php"
URL_v3 = "https://globalfundmedia13726.api-us1.com/api/3"

KEY = os.environ["AC_KEY"]

class Job(DailyJob):
    help = "Newsletter data generation."

    def execute(self):
        headers = {"Accept": "application/json", "api-token": KEY}

        # Get data
        cols = [
            "id",
            "date",
            "name",
            "subject",
            "fromemail",
            "email_category",
            "send_amt",
            "uniqueopens",
            "subscriberclicks",
            "open_rate",
            "click_rate",
            "click_through_rate",
            "estimated_num_clicks",
            "estimated_click_through_rate",
            "unsubscribes",
            "unsub_rate",
            "hardbounces",
            "softbounces",
            "weighted_bounce_rate",
            "day",
        ]

        # Request data using API

        # limit: number of results in a search (between 1 and 100)
        # offset: index of first result in a search
        # N: number of pages we want to search
        limit = 100
        N = 6

        campaign_ids = []   # list where we"ll store campaign IDs

        for i in range(N):

            # Define search parameters
            queryparams = {
                "limit": limit,
                "orders[sdate]": "DESC",
                "offset": i * limit,
            }

            response = requests.get(f"{URL_v3}/campaigns", headers=headers, params=queryparams)

            # Add campaign IDs to list
            campaign_ids.extend(
                [int(campaign["id"]) for campaign in response.json()["campaigns"]]
            )

        # List where we"ll store campaign data
        campaigns = []

        for campaignid in campaign_ids:
            try:
                campaign_data = get_campaign_data(campaignid, with_estimated_clicks=True)

                if campaign_data is not None:
                    campaigns.append(campaign_data)
            except Exception as e:
                pass

        # Create DataFrame
        df = pd.DataFrame(campaigns)[cols]

        # Set percent columns (for formatting)
        percent_cols = [
            "open_rate",
            "click_rate",
            "click_through_rate",
            "estimated_click_through_rate",
            "unsub_rate",
            "weighted_bounce_rate",
        ]

        # Attach link to campaign
        for col in ["id"]:

            temp = df[col]

            df[col] = (
                '=HYPERLINK("https://globalfundmedia13726.activehosted.com/report/#/campaign/'
                + (temp.astype(str))
                + '/overview", '
                + (temp.astype(str))
                + ")"
            )

        # Fix dates
        df["date"] = df["date"].dt.date

        # Split into the two editorial titles
        # NB regular newsletters and weekly "themed" newsletters are sometimes sent from different domains

        hw = df[
            (df["fromemail"] == "newsletters@hedgeweek.com")
            | (
                (df["fromemail"] == "newsletters@globalfundmedia.com")
                & (df["name"].str.contains("HW"))
            )
        ].reset_index(drop=True)

        pew = df[
            (df["fromemail"] == "newsletters@privateequitywire.co.uk")
            | (
                (df["fromemail"] == "newsletters@globalfundmedia.com")
                & (df["name"].str.contains("PE"))
            )
        ].reset_index(drop=True)

        # Separate out daily and themed newsletters
        hw.loc[hw["name"].str.contains("HW"), "newsletter_type"] = "Themed"
        hw["newsletter_type"].fillna("Daily", inplace=True)

        pew.loc[pew["name"].str.contains("PE"), "newsletter_type"] = "Themed"
        pew["newsletter_type"].fillna("Daily", inplace=True)

        # Generate and save open rate and CTR plots
        plot_daily_newsletter_data(hw, f"HW-newsletter", "HW", ["#FA7800", "#005247"])
        plot_daily_newsletter_data(pew, f"PEW-newsletter", "PEW", ["#8B0E04", "#389CB7"])

        plot_marketing_data(df, f"HW-marketing", "HW marketing", ["#FA7800", "#005247"])
        plot_marketing_data(df, f"PEW-marketing", "PEW marketing", ["#8B0E04", "#389CB7"])
        plot_marketing_data(df, f"Plain-text-email", "Plain-text email", ["#7C40B3", "#00A894"])

        # Write each DataFrame to file
        for title, data in [("hedgeweek", hw), ("privateequitywire", pew), ("all_campaigns", df)]:

            file_name = f"{title}.xlsx"
            writer = pd.ExcelWriter(
                settings.MEDIA_ROOT / f"newsletter_performance/{file_name}", engine="xlsxwriter",
            )

            data.to_excel(writer, index=False, sheet_name="Campaign data")

            workbook = writer.book
            worksheet = writer.sheets["Campaign data"]

            pct_fmt = workbook.add_format({"num_format": "0.00%"})

            # Set column widths
            for column in data:
                column_length = min(50, max(data[column].astype(str).map(len).max(), len(column)))
                col_idx = data.columns.get_loc(column)

                # Format percentages here as well
                if column in percent_cols:
                    worksheet.set_column(col_idx, col_idx, column_length, pct_fmt)

                elif column == "id":
                    worksheet.set_column(col_idx, col_idx, 6)

                elif column == "date":
                    worksheet.set_column(col_idx, col_idx, 15)

                else:
                    worksheet.set_column(col_idx, col_idx, column_length)

            # Freeze top row
            writer.sheets["Campaign data"].freeze_panes(1, 0)

            writer.close()

            regex = re.compile(f"{title}.*.xlsx")
            files = [file for file in os.listdir(settings.MEDIA_ROOT) if re.match(regex, file) and file != file_name]

            for file in files:
                os.remove(settings.MEDIA_ROOT / file)


# =============================
# UTILITY FUNCTIONS
# =============================

def get_campaign_data(campaignid, with_estimated_clicks=False):
    """Retrieve and clean relevant data for a particular ActiveCampaign campaign ID.

    Returns a DataFrame/dict with one row corresponding to that campaign.
    """

    # Define search parameters
    queryparams = {
        "api_action": "campaign_list",
        "api_key": KEY,
        "ids": campaignid,
        "api_output": "json",
    }

    response = requests.get(URL_v1, params=queryparams).json()["0"]

    # Define data we require
    key_list = [
        "id",
        "name",
        "sdate_iso",
        "send_amt",
        "opens",
        "uniqueopens",
        "subscriberclicks",
        "linkclicks",
        "uniquelinkclicks",
        "hardbounces",
        "softbounces",
        "unsubscribes",
        "unsubreasons",
        "userid",
        "messages",
    ]

    not_ints = ["name", "sdate_iso", "messages"]  # dictionary keys that aren"t numbers

    # Convert all numbers into ints
    campaign_dict = {
        k: int(response[k]) if k not in not_ints else response[k] for k in key_list
    }

    message = campaign_dict["messages"][0]  # message data within campaign

    # Add in useful message data
    campaign_dict["messageid"] = message["id"]
    campaign_dict["fromemail"] = message["fromemail"]
    campaign_dict["subject"] = message["subject"]
    campaign_dict["num_links"] = len(pd.DataFrame(message["links"]).drop_duplicates(subset=["link"]))

    campaign_dict.pop("messages", None) # remove "messages" key


    # Create DataFrame object for easier manipulation
    campaign_df = pd.DataFrame(campaign_dict, index=[0])

    # Define key aggregate metrics
    campaign_df["open_rate"] = (campaign_df["uniqueopens"] / campaign_df["send_amt"])
    campaign_df["click_rate"] = (campaign_df["subscriberclicks"] / campaign_df["send_amt"])
    campaign_df["click_through_rate"] = (campaign_df["subscriberclicks"] / campaign_df["uniqueopens"])
    campaign_df["unsub_rate"] = (campaign_df["unsubscribes"] / campaign_df["send_amt"])
    campaign_df["hard_bounce_rate"] = (campaign_df["hardbounces"] / campaign_df["send_amt"])
    campaign_df["soft_bounce_rate"] = (campaign_df["softbounces"] / campaign_df["send_amt"])
    campaign_df["weighted_bounce_rate"] = campaign_df["hard_bounce_rate"] + campaign_df["soft_bounce_rate"] / 3

    campaign_df["clicks_per_person"] = campaign_df["linkclicks"] / campaign_df["subscriberclicks"]
    campaign_df["opens_per_person"] = campaign_df["opens"] / campaign_df["uniqueopens"]

    campaign_df.fillna(0, inplace=True)

    # Fix datetime formats
    campaign_df["sdate"] = pd.to_datetime(campaign_df["sdate_iso"], utc=True)
    campaign_df.drop(columns="sdate_iso", inplace=True)

    campaign_df["fromemail"] = campaign_df["fromemail"].str.lower()

    # isolate email domains
    campaign_df["domain"] = campaign_df["fromemail"].str.split("@").str[-1]

    # categorise emails
    campaign_df["email_category"] = campaign_df[["fromemail", "name", "subject"]].apply(lambda x: get_email_category(*x), axis=1)

    # Reorder columns
    first_cols = ["id", "name", "subject", "fromemail"]  # first few columns
    campaign_df = campaign_df[
        first_cols + [col for col in campaign_df.columns if col not in first_cols]
    ]

    # Remove test emails and unsent emails
    campaign_df = campaign_df[campaign_df["send_amt"] > 10].reset_index(drop=True)

    # Break down time data
    campaign_df["day"] = campaign_df["sdate"].dt.dayofweek
    campaign_df["date"] = campaign_df["sdate"].dt.date.astype("datetime64")

    campaign_df["day"] = campaign_df["day"].replace(
        {
            0: "Monday",
            1: "Tuesday",
            2: "Wednesday",
            3: "Thursday",
            4: "Friday",
            5: "Saturday",
            6: "Sunday",
        },
        regex=True,
    )

    # Add in estimated number of clicks
    if with_estimated_clicks:

        try:
            if campaign_df["subscriberclicks"].iloc[0] > 0:
                campaign_df["estimated_num_clicks"] = estimated_num_clicks(campaignid)

                campaign_df["click_reduction_factor"] = campaign_df["estimated_num_clicks"] / campaign_df["subscriberclicks"]

                campaign_df["estimated_click_rate"] = campaign_df["click_reduction_factor"] * campaign_df["click_rate"]
                campaign_df["estimated_click_through_rate"] = campaign_df["click_reduction_factor"] * campaign_df["click_through_rate"]
            else:
                return None

        except IndexError as e:
            return None

    try:
        return campaign_df.iloc[0].to_dict()

    except IndexError:
        return None

def get_email_category(emailaddress, name, subject):
    """Categorise ActiveCampaign emails based on email addresses."""

    emailaddress = emailaddress.lower()
    if "noreply@hedgeweek" in emailaddress:
        return "HW marketing"
    if "noreply@privateequity" in emailaddress:
        return "PEW marketing"

    name, subject = name.lower(), subject.lower()

    if "newsletter" in emailaddress:
        if "talent tracker" in name:
            if "hedgeweek" in emailaddress:
                return "HW talent tracker"
            else:
                return "PEW talent tracker"
        elif "deal flow" in name or "deal flow" in subject:
            if "hedgeweek" in emailaddress:
                return "HW deal flow"
            else:
                return "PEW deal flow"
        else:
            if "hedgeweek" in emailaddress:
                return "HW daily"
            else:
                return "PEW daily"

    if "globalfundmedia.com" in emailaddress:
        return "Plain-text email"

    return "Other"


def estimated_num_clicks(
    campaignid, threshold=2, as_fraction=False
):
    """Returns the estimated number of clicks for a campaign"""
    links = get_campaign_clicks(campaignid)
    users = group_users_by_clicks(links)
    real_users = get_real_users(users, threshold=threshold)

    estimated_clicks = len(real_users)

    if as_fraction:
        try:
            return estimated_clicks / len(users)
        except ZeroDivisionError:
            return 0
    else:
        return estimated_clicks

def get_campaign_clicks(campaignid):
    """Get data on the different links --and who clicked them -- in a particular email.

    Returns a DataFrame with one link per row, along with associated information.
    """

    page = 1  # page number
    page_list = []  # store results from each page here

    while page > 0:

        # Define search parameters
        queryparams = {
            "api_action": "campaign_report_link_list",
            "api_key": KEY,
            "campaignid": campaignid,
            "api_output": "json",
            "page": page,
        }
        response = requests.get(URL_v1, params=queryparams).json()  # store response

        if response["result_code"]:  # check if request was successful

            # Add results from page to our list (and remove the last three rows, which contain information about the query -- not data)
            page_list.append(pd.DataFrame(response).T.iloc[:-3])

            page += 1  # move to next page

        else:
            break

    result = pd.concat(page_list).reset_index(drop=True)

    result = result[["name", "link", "a_unique", "info"]]

    return result

def get_real_users(
    users_df: pd.DataFrame, threshold: float = 2, return_link_checkers: bool = False,
) -> pd.DataFrame:
    """Return a DataFrame of "real users" based on a threshold (number of seconds between clicks)"""

    link_checkers = users_df[
        (users_df["num_clicks"] > 1) & (users_df["seconds_between_clicks"] < threshold)
    ]

    if return_link_checkers:
        return link_checkers
    else:
        # Return DataFrame of "real" users
        return users_df[~users_df["email"].isin(link_checkers["email"])]

def group_users_by_clicks(links_df: pd.DataFrame) -> pd.DataFrame:
    """Return DataFrame of users and how many links they clicked on and when"""

    contacts = {}  # store people who clicked

    N = len(links_df)  # number of links in the email

    # For each link, get data on who clicked, and organise by email
    for i in links_df.index:

        for user in links_df.iloc[i]["info"]:

            if user["email"] not in contacts.keys():

                # Create entry for that user
                contacts[user["email"]] = {
                    "num_clicks": 1,
                    "tstamps": [pd.to_datetime(user["tstamp_iso"], utc=True)],
                }

            else:

                # Update entry for that user
                entry = contacts[user["email"]]

                entry["num_clicks"] += 1
                entry["tstamps"].append(pd.to_datetime(user["tstamp_iso"], utc=True))

    # Convert to DataFrame
    contact_df = (
        pd.DataFrame(contacts).T.reset_index().rename(columns={"index": "email"})
    )

    contact_df["domain"] = contact_df["email"].str.split("@").str[-1]

    # Percentage of links they clicked
    try:
        contact_df["pct_of_clicks"] = (contact_df["num_clicks"] / N) * 100

    except KeyError:
        contact_df["num_clicks"] = None
        contact_df["pct_of_clicks"] = None
        contact_df["seconds_between_clicks"] = None

        return contact_df

    # Time between clicks
    contact_df["seconds_between_clicks"] = contact_df["tstamps"].apply(
        lambda x: (max(x) - min(x)).total_seconds() / (len(x))
    )

    return contact_df


def plot_daily_newsletter_data(
    data: pd.DataFrame,
    filename: str,
    editorial_title: str,
    colors: list = ["#7C40B3", "#00A894"],
    window: int = 5
) -> None:
    """Generate and save plots for daily HW/PEW newsletter metrics."""

    # Get only the daily newseltters (not the themed ones)
    daily = data[data["newsletter_type"] == "Daily"].sort_values("date").set_index("date")

    # Calculate rolling averages
    rolling = daily[["open_rate", "estimated_click_through_rate"]].rolling(window).mean()

    fig, ax = plt.subplots(figsize=(8, 5))

    # Plot {window}-day rolling average
    (rolling["open_rate"] * 100).plot(label="Open rate", marker=".", color=colors[0])
    (rolling["estimated_click_through_rate"] * 100).plot(label="Click-through rate", marker=".", color=colors[1])

    plt.legend(fancybox=True, framealpha=0)

    # Format axes
    plt.xlabel("")
    plt.xticks(rotation=0)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))

    try:
        plt.ylim(
            0,
            max(
                (rolling["open_rate"] * 100).max(),
                (rolling["estimated_click_through_rate"] * 100).max()
            ) * 1.2
        )
    except Exception:
        pass

    plt.title(f"{editorial_title} daily newsletter metrics, {window}-day rolling average")

    plt.savefig(f"{settings.BASE_DIR}/media/newsletter_performance/images/{filename}.png", dpi=500, transparent=True)

def plot_marketing_data(
    data: pd.DataFrame,
    filename: str,
    email_category: str,
    colors: list = ["#7C40B3", "#00A894"],
    window: int = 5
) -> None:
    """Generate and save plots for GFM marketing emails."""

    # Only keep emails from that category
    df = data[data["email_category"] == email_category].groupby("date").mean()

    # Get rolling averages
    rolling = df[["open_rate", "estimated_click_through_rate"]].rolling(window).mean()

    fig, ax = plt.subplots(figsize=(8, 5))

    # Plot {window}-day rolling average
    (rolling["open_rate"] * 100).plot(label="Open rate", marker=".", color=colors[0])
    (rolling["estimated_click_through_rate"] * 100).plot(label="Click-through rate", marker=".", color=colors[1])

    plt.legend(fancybox=True, framealpha=0)

    # Format axes
    plt.xlabel("")
    plt.xticks(rotation=0)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))

    try:
        plt.ylim(
            0,
            max(
                (rolling["open_rate"] * 100).max(),
                (rolling["estimated_click_through_rate"] * 100).max()
            ) * 1.2
        )
    except Exception:
        pass

    plt.title(f"Email metrics ({email_category}), {window}-day rolling average")

    plt.savefig(f"{settings.BASE_DIR}/media/newsletter_performance/images/{filename}.png", dpi=500, transparent=True)
