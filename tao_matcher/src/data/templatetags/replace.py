from django import template

register = template.Library()

@register.filter
def replace(value):
    if value:
        return value.replace("/", "\uffff")
    return value
