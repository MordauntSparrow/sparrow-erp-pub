import os
from pathlib import Path
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from flask import (
    Blueprint,
    request,
    render_template,
    redirect,
    url_for,
    flash,
    abort,
    Response,
    make_response,
)
from werkzeug.utils import secure_filename

from app.objects import PluginManager
from .objects import ArticleService

# =============================================================================
# Blueprints
# =============================================================================

internal_template_folder = os.path.join(os.path.dirname(__file__), "templates")

internal_bp = Blueprint(
    "internal_news_blog",
    __name__,
    url_prefix="/plugin/news_blog_module",
    template_folder=internal_template_folder,
)

plugin_manager = PluginManager(os.path.abspath("app/plugins"))
core_manifest = plugin_manager.get_core_manifest()

# =============================================================================
# Plugin settings helpers
# =============================================================================


def _get_plugin_manifest():
    try:
        return plugin_manager.get_plugin_manifest("news_blog_module")
    except Exception:
        return {}


def _get_setting(manifest, key, default=None):
    try:
        return (manifest or {}).get("settings", {}).get(key, {}).get("value", default)
    except Exception:
        return default


def _get_cfg():
    pm = _get_plugin_manifest()
    section_slug = (_get_setting(pm, "section_slug", "news")
                    or "news").strip().strip("/")
    return {
        "nav_item_label": _get_setting(pm, "nav_item_label", "News"),
        "section_slug": section_slug,
        "posts_per_page": int(_get_setting(pm, "posts_per_page", 10) or 10),
    }


def _company_name():
    try:
        return (core_manifest.get("site_settings", {}) or {}).get("company_name") or "Sparrow ERP"
    except Exception:
        return "Sparrow ERP"


# =============================================================================
# Admin helpers
# =============================================================================


def _app_static_dir():
    """
    Resolve app/static directory reliably from this plugin location:
      app/plugins/news_blog_module/routes.py -> app/static
    """
    plugin_dir = Path(__file__).resolve().parent
    app_dir = plugin_dir.parent.parent  # .../app
    return str(app_dir / "static")


def _news_upload_dir():
    """
    Store uploads under app/static so url_for('static', filename=...) works.
    """
    return os.path.join(_app_static_dir(), "uploads", "news_blog")


def _allowed_image_ext(filename: str) -> bool:
    ext = (os.path.splitext(filename or "")[1] or "").lower()
    return ext in (".jpg", ".jpeg", ".png", ".webp", ".gif")


def _unique_filename(upload_dir: str, filename: str) -> str:
    base, ext = os.path.splitext(filename)
    candidate = filename
    i = 2
    while os.path.exists(os.path.join(upload_dir, candidate)):
        candidate = f"{base}-{i}{ext}"
        i += 1
    return candidate


def _save_cover_image(file_storage):
    """
    Save file to app/static/uploads/news_blog/<filename>
    Return DB path relative to /static: uploads/news_blog/<filename>
    """
    if not file_storage:
        return None

    filename = secure_filename(file_storage.filename or "")
    if not filename:
        return None

    if not _allowed_image_ext(filename):
        flash("Cover image must be: .jpg, .jpeg, .png, .webp, .gif", "warning")
        return None

    upload_dir = _news_upload_dir()
    os.makedirs(upload_dir, exist_ok=True)

    filename = _unique_filename(upload_dir, filename)
    abs_path = os.path.join(upload_dir, filename)
    file_storage.save(abs_path)

    rel_path = f"uploads/news_blog/{filename}"
    return rel_path


def _parse_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default


def _slug_or_none(s):
    s = (s or "").strip()
    return s or None


def _build_article_payload_from_request():
    """
    Build the payload expected by ArticleService.create_article/update_article
    from request.form/request.files, matching your admin templates.
    """
    data = request.form.to_dict(flat=True)

    # Multi-select categories
    category_ids = request.form.getlist("category_ids")
    data["category_ids"] = category_ids

    # Optional primary category
    primary_category_id = (request.form.get(
        "primary_category_id") or "").strip()
    data["primary_category_id"] = primary_category_id or None

    # Tags: comma-separated names
    tag_names_raw = (request.form.get("tag_names") or "").strip()
    tag_names = [t.strip() for t in tag_names_raw.split(",")
                 if t.strip()] if tag_names_raw else []
    data["tag_names"] = tag_names

    # Status normalization
    status = (data.get("status") or "draft").strip().lower()
    data["status"] = status if status in ("draft", "published") else "draft"

    # Cover image upload (optional)
    cover_file = request.files.get("cover_image")
    if cover_file and cover_file.filename:
        saved_rel = _save_cover_image(cover_file)
        if saved_rel:
            data["cover_image_path"] = saved_rel

    # Alt text
    data["cover_image_alt"] = (
        data.get("cover_image_alt") or "").strip() or None

    return data


# =============================================================================
# Public helper: compatibility wrapper for get_public_articles
# =============================================================================


def _call_get_public_articles(page, page_size, category_slug=None, tag_slug=None, q=None):
    """
    Compatibility wrapper around ArticleService.get_public_articles.
    Keeps routes free of SQL and avoids hard-coding the objects.py signature.

    - If objects.py supports keyword/q/search, we pass it.
    - If not, we omit it (so /news still works) until objects.py is updated.
    """
    fn = getattr(ArticleService, "get_public_articles", None)
    if not fn:
        raise RuntimeError("ArticleService.get_public_articles is missing")

    # Best-effort introspection
    try:
        code = fn.__code__
        argnames = set(code.co_varnames[: code.co_argcount])
    except Exception:
        argnames = set()

    kwargs = {"page": page, "page_size": page_size}

    if category_slug:
        kwargs["category_slug"] = category_slug
    if tag_slug:
        kwargs["tag_slug"] = tag_slug

    if q:
        if "keyword" in argnames:
            kwargs["keyword"] = q
        elif "q" in argnames:
            kwargs["q"] = q
        elif "search" in argnames:
            kwargs["search"] = q
        # else: objects.py doesn't support search yet; we'll add it when you paste objects.py

    return fn(**kwargs)


# =============================================================================
# Date helpers (SEO/feeds)
# =============================================================================


def _to_datetime_utc(dt_val):
    """
    Best-effort convert dt_val (datetime or string) to timezone-aware UTC datetime.
    - If dt_val is naive datetime -> assume UTC
    - If dt_val is string:
        - try ISO 8601 (datetime.fromisoformat)
        - try RFC 2822 (parsedate_to_datetime)
        - else return None
    """
    if not dt_val:
        return None

    if isinstance(dt_val, datetime):
        dt = dt_val
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    s = str(dt_val).strip()
    if not s:
        return None

    # Try ISO-8601
    try:
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
        else:
            s2 = s
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # Try RFC 2822
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _iso8601(dt_val):
    dt = _to_datetime_utc(dt_val)
    if not dt:
        return None
    return dt.isoformat().replace("+00:00", "Z")


def _rfc2822(dt_val):
    dt = _to_datetime_utc(dt_val)
    if not dt:
        return None
    return dt.strftime("%a, %d %b %Y %H:%M:%S %z")


def _xml_escape(s):
    s = (s or "")
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _decorate_article_dates(article_dict: dict):
    """
    Add ISO strings used by templates/JSON-LD/OG.
    Mutates and returns the dict.
    """
    if not isinstance(article_dict, dict):
        return article_dict
    article_dict["published_at_iso"] = _iso8601(
        article_dict.get("published_at"))
    article_dict["updated_at_iso"] = _iso8601(article_dict.get("updated_at"))
    return article_dict


# =============================================================================
# Admin: Articles + SPA actions
# =============================================================================


@internal_bp.route("/", methods=["GET", "POST"])
def articles_list():
    cfg = _get_cfg()

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        # -------------------------
        # SPA: Categories
        # -------------------------
        if action == "create_category":
            name = (request.form.get("category_name") or "").strip()
            slug = _slug_or_none(request.form.get("category_slug"))
            if not name:
                flash("Category name is required.", "warning")
                return redirect(url_for("internal_news_blog.articles_list"))

            try:
                ArticleService.create_category(
                    name=name,
                    slug=slug,
                    description=None,
                    sort_order=0,
                )
                flash("Category created.", "success")
            except Exception as e:
                flash(f"Could not create category: {e}", "danger")

            return redirect(url_for("internal_news_blog.articles_list"))

        if action == "delete_category":
            category_id = (request.form.get("category_id") or "").strip()
            if not category_id:
                flash("Missing category_id.", "warning")
                return redirect(url_for("internal_news_blog.articles_list"))

            try:
                ArticleService.delete_category(category_id)
                flash("Category deleted.", "success")
            except Exception as e:
                flash(f"Could not delete category: {e}", "danger")

            return redirect(url_for("internal_news_blog.articles_list"))

        # -------------------------
        # SPA: Tags
        # -------------------------
        if action == "create_tag":
            name = (request.form.get("tag_name") or "").strip()
            slug = _slug_or_none(request.form.get("tag_slug"))
            if not name:
                flash("Tag name is required.", "warning")
                return redirect(url_for("internal_news_blog.articles_list"))

            try:
                ArticleService.create_tag(name=name, slug=slug)
                flash("Tag created.", "success")
            except Exception as e:
                flash(f"Could not create tag: {e}", "danger")

            return redirect(url_for("internal_news_blog.articles_list"))

        if action == "delete_tag":
            tag_id = (request.form.get("tag_id") or "").strip()
            if not tag_id:
                flash("Missing tag_id.", "warning")
                return redirect(url_for("internal_news_blog.articles_list"))

            try:
                ArticleService.delete_tag(tag_id)
                flash("Tag deleted.", "success")
            except Exception as e:
                flash(f"Could not delete tag: {e}", "danger")

            return redirect(url_for("internal_news_blog.articles_list"))

        # -------------------------
        # SPA: Articles
        # -------------------------
        if action == "delete_article":
            article_id = (request.form.get("article_id") or "").strip()
            if not article_id:
                flash("Missing article_id.", "warning")
                return redirect(url_for("internal_news_blog.articles_list"))

            try:
                ArticleService.delete_article(article_id)
                flash("Article deleted.", "success")
            except Exception as e:
                flash(f"Could not delete article: {e}", "danger")

            return redirect(url_for("internal_news_blog.articles_list"))

        if action == "update_article":
            article_id = (request.form.get("article_id") or "").strip()
            if not article_id:
                flash("Missing article_id.", "warning")
                return redirect(url_for("internal_news_blog.articles_list"))

            data = _build_article_payload_from_request()

            try:
                ArticleService.update_article(article_id, data)
                flash("Article updated.", "success")
            except Exception as e:
                flash(f"Save failed: {e}", "danger")

            return redirect(url_for("internal_news_blog.articles_list"))

        if action == "create_article":
            data = _build_article_payload_from_request()

            try:
                ArticleService.create_article(data)
                flash("Article created.", "success")
            except Exception as e:
                flash(f"Save failed: {e}", "danger")

            return redirect(url_for("internal_news_blog.articles_list"))

        flash(f"Unknown action: {action}", "warning")
        return redirect(url_for("internal_news_blog.articles_list"))

    # GET: admin list/search
    q = request.args.get("q")
    status = request.args.get("status")  # draft/published/None
    page = _parse_int(request.args.get("page"), 1) or 1
    page_size = _parse_int(request.args.get("page_size"), 20) or 20

    results = ArticleService.search_articles(
        keyword=q, status=status, page=page, page_size=page_size)
    kpis = ArticleService.get_article_kpis()
    categories = ArticleService.get_all_categories()
    tags = ArticleService.get_all_tags()

    return render_template(
        "admin/articles_list.html",
        articles=results["items"],
        total=results["total"],
        page=results["page"],
        page_size=results["page_size"],
        q=q,
        status=status,
        config=core_manifest,
        kpis=kpis,
        nav_item_label=cfg["nav_item_label"],
        section_slug=cfg["section_slug"],
        categories=categories,
        tags=tags,
        all_tags=tags,
    )


@internal_bp.post("/delete/<int:article_id>")
def admin_article_delete(article_id):
    # Backward compatibility route (your template uses SPA delete_article now)
    try:
        ArticleService.delete_article(article_id)
        flash("Article deleted.", "success")
    except Exception as e:
        flash(f"Could not delete article: {e}", "danger")
    return redirect(url_for("internal_news_blog.articles_list"))


@internal_bp.get("/details/<int:article_id>")
def admin_article_detail(article_id):
    cfg = _get_cfg()
    article = ArticleService.get_article_by_id(
        article_id, include_taxonomy=True)
    if not article:
        flash("Article not found.", "error")
        return redirect(url_for("internal_news_blog.articles_list"))

    return render_template(
        "admin/article_detail.html",
        article=article,
        config=core_manifest,
        nav_item_label=cfg["nav_item_label"],
        section_slug=cfg["section_slug"],
    )


# =============================================================================
# Admin: Categories/Tags (backward compatibility)
# =============================================================================


@internal_bp.route("/categories", methods=["GET", "POST"])
def admin_categories():
    cfg = _get_cfg()

    if request.method == "POST":
        data = request.form.to_dict(flat=True)
        action = (data.get("action") or "").strip().lower()

        if action == "create":
            ArticleService.create_category(
                name=data.get("name"),
                slug=data.get("slug"),
                description=data.get("description"),
                sort_order=data.get("sort_order") or 0,
            )
            flash("Category created.", "success")
        elif action == "update":
            ArticleService.update_category(
                category_id=data.get("category_id"),
                name=data.get("name"),
                slug=data.get("slug"),
                description=data.get("description"),
                sort_order=data.get("sort_order") or 0,
            )
            flash("Category updated.", "success")
        elif action == "delete":
            ArticleService.delete_category(data.get("category_id"))
            flash("Category deleted.", "success")

        return redirect(url_for("internal_news_blog.admin_categories"))

    categories = ArticleService.get_all_categories()
    return render_template(
        "admin/categories_list.html",
        categories=categories,
        config=core_manifest,
        nav_item_label=cfg["nav_item_label"],
        section_slug=cfg["section_slug"],
    )


@internal_bp.route("/tags", methods=["GET", "POST"])
def admin_tags():
    cfg = _get_cfg()

    if request.method == "POST":
        data = request.form.to_dict(flat=True)
        action = (data.get("action") or "").strip().lower()

        if action == "create":
            ArticleService.create_tag(
                name=data.get("name"), slug=data.get("slug"))
            flash("Tag created.", "success")
        elif action == "update":
            ArticleService.update_tag(
                tag_id=data.get("tag_id"),
                name=data.get("name"),
                slug=data.get("slug"),
            )
            flash("Tag updated.", "success")
        elif action == "delete":
            ArticleService.delete_tag(data.get("tag_id"))
            flash("Tag deleted.", "success")

        return redirect(url_for("internal_news_blog.admin_tags"))

    tags = ArticleService.get_all_tags()
    return render_template(
        "admin/tags_list.html",
        tags=tags,
        config=core_manifest,
        nav_item_label=cfg["nav_item_label"],
        section_slug=cfg["section_slug"],
    )


def get_blueprint():
    return internal_bp


# =============================================================================
# Public blueprint (dynamic url_prefix from section_slug)
# =============================================================================


def get_public_blueprint():
    """
    Public blueprint created at startup so url_prefix can use section_slug.
    Restart required to apply slug changes (acceptable).
    """
    cfg = _get_cfg()
    section_slug = cfg["section_slug"]

    public_bp = Blueprint(
        "public_news_blog",
        __name__,
        url_prefix=f"/{section_slug}",
        template_folder=internal_template_folder,
    )

    # -------------------------
    # RSS feed
    # -------------------------
    @public_bp.get("/rss.xml")
    def public_rss_feed():
        company = _company_name()
        feed_title = f"{company} – {cfg['nav_item_label']}"
        channel_link = url_for(
            "public_news_blog.public_articles_list", _external=True)

        items = ArticleService.get_public_articles(
            page=1, page_size=20).get("items", [])

        parts = []
        parts.append('<?xml version="1.0" encoding="UTF-8"?>')
        parts.append('<rss version="2.0">')
        parts.append("<channel>")
        parts.append(f"<title>{_xml_escape(feed_title)}</title>")
        parts.append(f"<link>{_xml_escape(channel_link)}</link>")
        parts.append(f"<description>{_xml_escape(feed_title)}</description>")
        parts.append("<language>en</language>")

        for a in items:
            a = _decorate_article_dates(a or {})
            item_link = url_for(
                "public_news_blog.public_article_detail",
                slug=a.get("slug"),
                _external=True,
            )
            title = a.get("title") or ""
            summary = a.get("summary") or ""
            pub = _rfc2822(a.get("published_at"))

            parts.append("<item>")
            parts.append(f"<title>{_xml_escape(title)}</title>")
            parts.append(f"<link>{_xml_escape(item_link)}</link>")
            parts.append(
                f"<guid isPermaLink='true'>{_xml_escape(item_link)}</guid>")
            if pub:
                parts.append(f"<pubDate>{_xml_escape(pub)}</pubDate>")
            if summary:
                parts.append(
                    f"<description>{_xml_escape(summary)}</description>")

            # Optional enclosure for cover image (best-effort)
            cover_path = a.get("cover_image_path")
            if cover_path:
                cover_url = url_for(
                    "static", filename=cover_path, _external=True)
                ext = os.path.splitext(cover_path)[1].lower()
                mime = {
                    ".jpg": "image/jpeg",
                    ".jpeg": "image/jpeg",
                    ".png": "image/png",
                    ".webp": "image/webp",
                    ".gif": "image/gif",
                }.get(ext, "image/jpeg")
                parts.append(
                    f"<enclosure url='{_xml_escape(cover_url)}' type='{mime}' />"
                )

            parts.append("</item>")

        parts.append("</channel>")
        parts.append("</rss>")

        xml = "\n".join(parts)
        resp = Response(xml, mimetype="application/rss+xml; charset=utf-8")
        resp.headers["Cache-Control"] = "public, max-age=300"
        return resp

    # -------------------------
    # Plugin sitemap (for aggregation by website module)
    # -------------------------

    @public_bp.route("/sitemap.xml", methods=["GET"])
    def public_sitemap():
        """
        Plugin-level sitemap for this section.
        Website module should aggregate this into the root sitemap index.

        For now: build normal external URLs, then force https by string replace.
        """
        items = ArticleService.get_public_articles(
            page=1, page_size=5000).get("items", [])

        try:
            categories = ArticleService.get_all_categories() or []
        except Exception:
            categories = []

        try:
            tags = ArticleService.get_all_tags() or []
        except Exception:
            tags = []

        def _force_https(u: str) -> str:
            u = (u or "").strip()
            if u.startswith("http://"):
                return "https://" + u[len("http://"):]
            return u

        def _abs(endpoint: str, **values) -> str:
            return _force_https(url_for(endpoint, _external=True, **values))

        def _url_entry(loc, lastmod=None):
            parts2 = [f"<url><loc>{_xml_escape(loc)}</loc>"]
            if lastmod:
                parts2.append(f"<lastmod>{_xml_escape(lastmod)}</lastmod>")
            parts2.append("</url>")
            return "".join(parts2)

        urlset = []
        urlset.append('<?xml version="1.0" encoding="UTF-8"?>')
        urlset.append(
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

        # Section index
        urlset.append(_url_entry(
            _abs("public_news_blog.public_articles_list")))

        # Category pages
        for c in categories:
            slug = (c.get("slug") or "").strip()
            if not slug:
                continue
            urlset.append(
                _url_entry(
                    _abs(
                        "public_news_blog.public_articles_by_category",
                        category_slug=slug,
                    )
                )
            )

        # Tag pages
        for t in tags:
            slug = (t.get("slug") or "").strip()
            if not slug:
                continue
            urlset.append(
                _url_entry(
                    _abs(
                        "public_news_blog.public_articles_by_tag",
                        tag_slug=slug,
                    )
                )
            )

        # Article canonicals
        for a in items:
            a = _decorate_article_dates(a or {})
            slug = (a.get("slug") or "").strip()
            if not slug:
                continue
            lastmod = a.get("updated_at_iso") or a.get("published_at_iso")
            urlset.append(
                _url_entry(
                    _abs(
                        "public_news_blog.public_article_detail",
                        slug=slug,
                    ),
                    lastmod=lastmod,
                )
            )

        urlset.append("</urlset>")

        xml = "\n".join(urlset)
        resp = Response(xml, mimetype="application/xml; charset=utf-8")
        resp.headers["Cache-Control"] = "public, max-age=300"
        return resp

    # -------------------------
    # Public list
    # -------------------------

    @public_bp.get("/")
    def public_articles_list():
        page = _parse_int(request.args.get("page"), 1) or 1
        page_size = _parse_int(request.args.get(
            "page_size"), cfg["posts_per_page"]) or cfg["posts_per_page"]
        q = (request.args.get("q") or "").strip() or None

        results = _call_get_public_articles(
            page=page, page_size=page_size, q=q)

        items = [_decorate_article_dates(a)
                 for a in (results.get("items") or [])]

        return render_template(
            "public/articles_list.html",
            articles=items,
            total=results.get("total"),
            page=results.get("page") or page,
            page_size=results.get("page_size") or page_size,
            q=q,
            config=core_manifest,
            nav_item_label=cfg["nav_item_label"],
            section_slug=cfg["section_slug"],
            filter_title=None,
        )

    # -------------------------
    # Tag listing
    # -------------------------
    @public_bp.get("/tag/<tag_slug>")
    def public_articles_by_tag(tag_slug):
        page = _parse_int(request.args.get("page"), 1) or 1
        page_size = _parse_int(request.args.get(
            "page_size"), cfg["posts_per_page"]) or cfg["posts_per_page"]
        q = (request.args.get("q") or "").strip() or None

        tag = ArticleService.get_tag_by_slug(tag_slug)
        if not tag:
            abort(404)

        results = _call_get_public_articles(
            page=page, page_size=page_size, tag_slug=tag_slug, q=q)
        items = [_decorate_article_dates(a)
                 for a in (results.get("items") or [])]

        return render_template(
            "public/articles_list.html",
            articles=items,
            total=results.get("total"),
            page=results.get("page") or page,
            page_size=results.get("page_size") or page_size,
            q=q,
            tag=tag,
            config=core_manifest,
            nav_item_label=cfg["nav_item_label"],
            section_slug=cfg["section_slug"],
            filter_title=f"Tag: {tag.get('name')}",
        )

    # -------------------------
    # Category listing (or fallback to article detail)
    # -------------------------
    @public_bp.get("/<category_slug>")
    def public_articles_by_category(category_slug):
        category = ArticleService.get_category_by_slug(category_slug)
        if not category:
            return public_article_detail(category_slug)

        page = _parse_int(request.args.get("page"), 1) or 1
        page_size = _parse_int(request.args.get(
            "page_size"), cfg["posts_per_page"]) or cfg["posts_per_page"]
        q = (request.args.get("q") or "").strip() or None

        results = _call_get_public_articles(
            page=page,
            page_size=page_size,
            category_slug=category_slug,
            q=q,
        )
        items = [_decorate_article_dates(a)
                 for a in (results.get("items") or [])]

        return render_template(
            "public/articles_list.html",
            articles=items,
            total=results.get("total"),
            page=results.get("page") or page,
            page_size=results.get("page_size") or page_size,
            q=q,
            config=core_manifest,
            nav_item_label=cfg["nav_item_label"],
            section_slug=cfg["section_slug"],
            filter_title=category.get("name"),
            category=category,
        )

    # -------------------------
    # Category/article canonical redirect
    # -------------------------
    @public_bp.get("/<category_slug>/<article_slug>")
    def public_article_detail_in_category(category_slug, article_slug):
        return redirect(
            url_for("public_news_blog.public_article_detail",
                    slug=article_slug, _external=True),
            code=301,
        )

    # -------------------------
    # Article detail
    # -------------------------
    @public_bp.get("/<slug>")
    def public_article_detail(slug):
        article = ArticleService.get_public_article_by_slug(
            slug, include_taxonomy=True)
        if not article:
            abort(404)

        article = _decorate_article_dates(article)

        # Conditional GET / Last-Modified
        last_mod_dt = _to_datetime_utc(article.get(
            "updated_at") or article.get("published_at"))

        if last_mod_dt:
            ims = request.headers.get("If-Modified-Since")
            if ims:
                try:
                    ims_dt = parsedate_to_datetime(ims)
                    if ims_dt.tzinfo is None:
                        ims_dt = ims_dt.replace(tzinfo=timezone.utc)
                    ims_dt = ims_dt.astimezone(timezone.utc)

                    if last_mod_dt <= ims_dt:
                        resp = make_response("", 304)
                        resp.headers["Last-Modified"] = last_mod_dt.strftime(
                            "%a, %d %b %Y %H:%M:%S %z")
                        resp.headers["Cache-Control"] = "public, max-age=300"
                        return resp
                except Exception:
                    pass

        # View tracking (only on full response)
        try:
            ArticleService.increment_article_view(article["id"])
        except Exception:
            pass

        resp = make_response(
            render_template(
                "public/article_detail.html",
                article=article,
                config=core_manifest,
                nav_item_label=cfg["nav_item_label"],
                section_slug=cfg["section_slug"],
            )
        )

        if last_mod_dt:
            resp.headers["Last-Modified"] = last_mod_dt.strftime(
                "%a, %d %b %Y %H:%M:%S %z")
            resp.headers["Cache-Control"] = "public, max-age=300"

        return resp

    return public_bp
