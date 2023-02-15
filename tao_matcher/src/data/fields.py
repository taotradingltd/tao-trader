AUM_CATEGORY = [
    (0, "—"),
    (1, "< USD 1bn"),
    (2, "USD 1bn - 5bn"),
    (3, "USD 5bn - 10bn"),
    (4, "USD 10bn+")
]

AUM_BRACKET = [
    (0, "—"),
    (1, "< USD 1bn"),
    (2, "USD 1bn - 5bn"),
    (3, "USD 5bn - 10bn"),
    (4, "USD 10bn+"),
]

EMAIL_FORMAT_LAMBDA_MAP = {
    "first.last": lambda first, last, domain: f"{first}.{last}@{domain}".lower().strip(),  # john.smith@globalfundmedia.com
    "first": lambda first, last, domain: f"{first}@{domain}".lower().strip(),              # john@globalfundmedia.com
    "flast": lambda first, last, domain: f"{first[0]}{last}@{domain}".lower().strip(),     # jsmith@globalfundmedia.com
    "fl": lambda first, last, domain: f"{first[0]}{last[0]}@{domain}".lower().strip(),     # js@globalfundmedia.com
    "firstlast": lambda first, last, domain: f"{first}{last}@{domain}".lower().strip(),    # johnsmith@globalfundmedia.com
    "f.last": lambda first, last, domain: f"{first[0]}.{last}@{domain}".lower().strip(),   # j.smith@globalfundmedia.com
    "first_last": lambda first, last, domain: f"{first}_{last}@{domain}".lower().strip(),  # john_smith@globalfundmedia.com
    "firstl": lambda first, last, domain: f"{first}{last[0]}@{domain}".lower().strip(),    # johns@globalfundmedia.com
    "last": lambda first, last, domain: f"{last}@{domain}".lower().strip(),                # smith@globalfundmedia.com
    "f.l": lambda first, last, domain: f"{first[0]}.{last[0]}@{domain}".lower().strip(),   # j.s@globalfundmedia.com
    "lastfirst": lambda first, last, domain: f"{last}{first}@{domain}".lower().strip(),    # smithjohn@globalfundmedia.com
    "lastf": lambda first, last, domain: f"{last}{first[0]}@{domain}".lower().strip(),     # smithj@globalfundmedia.com
    "last.first": lambda first, last, domain: f"{last}.{first}@{domain}".lower().strip(),  # smith.john@globalfundmedia.com
    "first.l": lambda first, last, domain: f"{first}.{last[0]}@{domain}".lower().strip(),  # john.s@globalfundmedia.com
    "fla": lambda first, last, domain: f"{first}{last[:2]}@{domain}".lower().strip(),      # jsm@globalfundmedia.com
}
