import django_tables2 as tables

from .models import Article

class ArticleTable(tables.Table):
    publish = tables.BooleanColumn(
        accessor="publish", null=False,
        verbose_name="Published?"
    )

    select = tables.CheckBoxColumn(
        accessor="pk", empty_values=(), attrs={
            "th__input": {"onclick": "toggle(this)"},
            "input": {"form": "useful"}
        }, orderable=False)

    url = tables.TemplateColumn("""
    {% if record.has_url %}
        {% if record.has_source_name %}
            <a href="{{ record.url }}" target="_blank">{{ record.get_source_name }}</a>
        {% else %}
            <a href="{{ record.url }}" target="_blank">{{ record.url|truncatechars:25 }}</a>
        {% endif %}
    {% else %}
        {{ record.get_source_name }}
    {% endif %}
    """, verbose_name="Source")

    # TODO: re-add this once the summarize functionality works sufficiently
    # summarize = tables.LinkColumn("summarize_article", text="Generate summary", verbose_name="", args=[tables.A("pk")])

    editorial_title = tables.Column(accessor="editorial_title", verbose_name="Publish to")
    editable = tables.LinkColumn("edit_article", text="Edit article", verbose_name="", args=[tables.A("pk")])
    delete = tables.LinkColumn("delete_article", text="Delete article", verbose_name="", args=[tables.A("pk")])

    def render_title(self, value):
        return " ".join(value.split()[:20]) + "..." if len(value.split()) > 20 else value

    class Meta:
        model = Article
        # Set order of fields in table
        fields = (
            "select", "publish", "editorial_title", "title", "url",
            "publish_date", "date_modified", "date_added"
        )
        # Exluded fields in table rendering
        exclude = ("content", "author", "tags", "source")
