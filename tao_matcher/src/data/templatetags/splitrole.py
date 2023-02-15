import re

from django import template

register = template.Library()

@register.filter
def splitrole(value):
    num_roles = value.replace(re.sub("\([\d]+ current roles\)", "", value), "")
    arr = value.split(" at ")
    return [x.replace(num_roles, "").strip() for x in arr] + [num_roles]
