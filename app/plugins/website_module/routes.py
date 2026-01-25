import mimetypes
import os
import json
import datetime
from datetime import date
from pathlib import Path

import markdown
from jinja2 import TemplateNotFound

from flask import (
    Blueprint,
    render_template,
    request,
    jsonify,
    make_response,
    url_for,
    render_template_string,
    send_from_directory,
    redirect,
    flash,
    current_app,
    Response,
)

from app.objects import PluginManager, EmailManager
from .objects import *  # noqa: F403


# ----------------------------
# Blueprints
# ----------------------------

website_public_routes = Blueprint(
    'website_public',
    __name__,
    template_folder=os.path.join(
        os.path.dirname(__file__), 'templates', 'public'),
    url_prefix='/',
    static_folder=os.path.join(os.path.dirname(__file__), 'static'),
    static_url_path='/website_module_static'
)

# NOTE: This blueprint appears unused in the provided code; kept for compatibility.
website_public_added_routes = Blueprint(
    'website_public_added',
    __name__,
    template_folder=os.path.join(
        os.path.dirname(__file__), 'templates', 'public'),
    url_prefix='/',
    static_folder=os.path.join(os.path.dirname(__file__), 'static'),
    static_url_path='/website_module_static'
)

templates_dir = os.path.join(os.path.dirname(__file__), 'templates/public')


# ----------------------------
# Analytics init (safe)
# ----------------------------
analytics = None
try:
    module_dir = os.path.dirname(os.path.abspath(__file__))
    from .objects import ensure_data_folder  # noqa: F401
    data_dir = ensure_data_folder(module_dir)  # noqa: F405
    analytics = AnalyticsManager(data_dir)  # noqa: F405
except Exception as e:
    print(f"[Website] Analytics disabled: {e}")
    analytics = None


# ----------------------------
# Path helpers (IMPORTANT)
# ----------------------------

def _plugins_dir() -> str:
    """
    This file lives at: app/plugins/website_module/routes.py
    So plugins dir is: app/plugins
    """
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _get_plugin_manager() -> PluginManager:
    return PluginManager(_plugins_dir())


# ----------------------------
# Manifest helpers
# ----------------------------

def get_core_manifest():
    """
    Returns the core manifest via PluginManager.
    Path-safe (no hardcoded 'app/plugins').
    """
    try:
        return _get_plugin_manager().get_core_manifest() or {}
    except Exception as e:
        print(f"[Website] get_core_manifest failed: {e}")
        return {}


def load_website_manifest():
    manifest_path = os.path.join(os.path.dirname(__file__), 'manifest.json')
    if os.path.exists(manifest_path):
        with open(manifest_path, encoding='utf-8') as f:
            return json.load(f)
    return {}


def get_website_settings():
    manifest = load_website_manifest()
    settings = manifest.get('settings', {}) or {}

    raw = (settings.get('schema_json') or '').strip()

    try:
        # Case 1: stored as a JSON string literal (starts/ends with quotes)
        if raw.startswith('"') and raw.endswith('"'):
            raw = json.loads(raw)  # unwraps the string

        # Case 2: stored as JSON text but still contains lots of \" (double-escaped)
        # If it doesn't start with { but looks escaped, try one more decode
        if not raw.lstrip().startswith('{') and '\\"@context\\"' in raw:
            raw = json.loads(f'"{raw}"')

        # Final: ensure it is valid JSON and output clean JSON
        raw = json.dumps(json.loads(raw), ensure_ascii=False)

    except Exception as e:
        print("[Schema DEBUG] normalize failed:", e)

    settings['schema_json'] = raw
    return settings


# ----------------------------
# URL / SEO helpers
# ----------------------------

def _abs_url(base: str, path: str) -> str:
    base = (base or '').rstrip('/')
    path = (path or '').strip()
    if not path.startswith('/'):
        path = '/' + path
    return base + path


def _iso_date_from_mtime(path: str):
    try:
        if path and os.path.exists(path):
            ts = os.path.getmtime(path)
            return datetime.datetime.utcfromtimestamp(ts).date().isoformat()
    except Exception:
        return None
    return None


def _load_pages_json():
    pages_file = os.path.join(os.path.dirname(__file__), 'pages.json')
    try:
        if os.path.exists(pages_file):
            with open(pages_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[Website] Failed to load pages.json: {e}")
    return []


def _record_page_view_safe():
    if not analytics:
        return
    try:
        analytics.record_page_view(  # noqa: F405
            page=request.path,
            ip_address=request.remote_addr,
            user_agent=request.user_agent.string,
            referrer=request.referrer
        )
    except Exception as e:
        print(f"[Website] Analytics error: {e}")


def _get_enabled_plugin_public_meta():
    """
    Collects public discovery metadata from enabled plugins.

    Expected plugin manifest fields (to be added across plugins):
      - public_sections: [{label, description, url, order}]
      - public_sitemaps: [{url}] or list of dicts or list of strings

    Defensive: if PluginManager doesn't include custom keys in get_all_plugins()
    output, it returns empty lists until plugins are updated.
    """
    try:
        pm = _get_plugin_manager()
        plugins = pm.get_all_plugins() or []
    except Exception as e:
        print(f"[Website] PluginManager get_all_plugins failed: {e}")
        plugins = []

    enabled = [p for p in plugins if isinstance(p, dict) and p.get('enabled')]

    sections = []
    sitemaps = []

    for p in enabled:
        for sec in (p.get('public_sections') or []):
            if not isinstance(sec, dict):
                continue
            label = (sec.get('label') or '').strip()
            url = (sec.get('url') or '').strip()
            if not label or not url:
                continue
            if not url.startswith('/'):
                url = '/' + url.lstrip('/')

            order_raw = sec.get('order')
            try:
                order = int(order_raw) if order_raw is not None else 1000
            except Exception:
                order = 1000

            sections.append({
                'label': label,
                'description': (sec.get('description') or '').strip(),
                'url': url,
                'order': order,
            })

        for sm in (p.get('public_sitemaps') or []):
            if isinstance(sm, str):
                u = sm.strip()
            elif isinstance(sm, dict):
                u = (sm.get('url') or '').strip()
            else:
                u = ''
            if not u:
                continue
            if not u.startswith('/'):
                u = '/' + u.lstrip('/')
            sitemaps.append(u)

    sections.sort(key=lambda x: (x['order'], x['label'].lower()))

    # de-dupe sitemaps preserving order
    seen = set()
    uniq_sitemaps = []
    for u in sitemaps:
        if u in seen:
            continue
        seen.add(u)
        uniq_sitemaps.append(u)

    return sections, uniq_sitemaps


# ----------------------------
# Public routes
# ----------------------------

@website_public_routes.route('/')
def root_page():
    pages = _load_pages_json()
    page_data = next((p for p in pages if p.get('route') == '/'), None)

    _record_page_view_safe()

    if not page_data:
        return "Home page not found.", 404

    template_file = 'index.html'
    template_path = os.path.join(os.path.dirname(
        __file__), 'templates', 'public', template_file)
    if not os.path.exists(template_path):
        return "Home page file is missing.", 404

    return render_template(
        template_file,
        page_data=page_data,
        config=get_core_manifest(),
        website_settings=get_website_settings(),
        pages=pages
    )


# --- Sitemaps (dynamic) ---

@website_public_routes.route('/sitemap')
@website_public_routes.route('/sitemap.xml')
def sitemap_index():
    """
    Dynamic sitemap INDEX:
      - /sitemaps/pages.xml (pages.json)
      - plugin-provided sitemaps declared via plugin manifest public_sitemaps
    """
    base_url = request.url_root.rstrip('/').replace('http://', 'https://', 1)

    _sections, plugin_sitemaps = _get_enabled_plugin_public_meta()

    xml = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml.append(
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

    # Website pages sitemap
    xml.append('  <sitemap>')
    xml.append(f'    <loc>{_abs_url(base_url, "/sitemaps/pages.xml")}</loc>')
    xml.append('  </sitemap>')

    # Plugin sitemaps
    for sm in plugin_sitemaps:
        xml.append('  <sitemap>')
        xml.append(f'    <loc>{_abs_url(base_url, sm)}</loc>')
        xml.append('  </sitemap>')

    xml.append('</sitemapindex>')
    return Response("\n".join(xml), mimetype='application/xml')


@website_public_routes.route('/sitemaps/pages.xml')
def sitemap_pages():
    """
    Dynamic pages sitemap from pages.json.
    Includes <lastmod> based on template file mtime (pragmatic fallback).
    """
    pages = _load_pages_json()
    base_url = request.url_root.rstrip('/').replace('http://', 'https://', 1)
    public_templates_dir = os.path.join(
        os.path.dirname(__file__), 'templates', 'public')

    xml = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

    for p in pages:
        route = (p.get('route') or '').strip() or '/'
        if not route.startswith('/'):
            route = '/' + route.lstrip('/')

        loc = _abs_url(base_url, route)

        # lastmod from template mtime
        if route == '/':
            tpl_path = os.path.join(public_templates_dir, 'index.html')
        else:
            tpl_path = os.path.join(
                public_templates_dir, f"{route.strip('/')}.html")
        lastmod = _iso_date_from_mtime(tpl_path)

        xml.append('  <url>')
        xml.append(f'    <loc>{loc}</loc>')
        if lastmod:
            xml.append(f'    <lastmod>{lastmod}</lastmod>')
        xml.append('  </url>')

    xml.append('</urlset>')
    return Response("\n".join(xml), mimetype='application/xml')


@website_public_routes.route('/robots.txt')
def robots_txt():
    lines = [
        "User-agent: *",
        "Disallow:",
        f"Sitemap: {request.url_root.rstrip('/')}/sitemap.xml"
    ]
    return Response('\n'.join(lines), mimetype='text/plain')


@website_public_routes.route('/llms.txt')
def llms_txt():
    """
    Dynamic llms.txt:
    - Human-first labels (no 'module' wording)
    - Lists key pages from pages.json
    - Lists enabled plugin sections from plugin manifests (public_sections)
    """
    base_url = request.url_root.rstrip('/').replace('http://', 'https://', 1)
    pages = _load_pages_json()
    sections, _plugin_sitemaps = _get_enabled_plugin_public_meta()

    core = get_core_manifest() or {}
    site_name = core.get('company_name') or 'Website'

    lines = []
    lines.append(f"# {site_name}")
    lines.append(f"# Base: {base_url}/")
    lines.append("")
    lines.append(f"Sitemap: {_abs_url(base_url, '/sitemap.xml')}")
    lines.append("")

    lines.append("## Key pages")
    for p in pages:
        route = (p.get('route') or '').strip() or '/'
        if not route.startswith('/'):
            route = '/' + route.lstrip('/')
        title = (p.get('title') or route).strip()
        lines.append(f"- {title}: {_abs_url(base_url, route)}")

    if sections:
        lines.append("")
        lines.append("## Content sections")
        for s in sections:
            lines.append(f"- {s['label']}: {_abs_url(base_url, s['url'])}")
            if s.get('description'):
                lines.append(f"  - {s['description']}")

    return Response("\n".join(lines) + "\n", mimetype='text/plain')


@website_public_routes.route('/<path:page_route>')
def custom_page(page_route):
    pages = _load_pages_json()
    page_data = next((p for p in pages if (p.get('route') or '').strip(
        '/').strip() == page_route.strip('/')), None)

    _record_page_view_safe()

    if not page_data:
        return "Page Not Found", 404

    template_file = f"{page_route.strip('/')}.html"
    return render_template(
        template_file,
        pages=pages,
        page_data=page_data,
        config=get_core_manifest(),
        website_settings=get_website_settings()
    )


@website_public_routes.route('/submit_form', methods=['POST'])
def form_submit():
    core_manifest = get_core_manifest()

    spam_protector = SpamProtection(core_manifest)  # noqa: F405
    is_spam, reason = spam_protector.is_spam(
        request.form,
        remote_ip=request.remote_addr or ""
    )
    if is_spam:
        flash("Spam detected: " + (reason or "Unknown reason"), "danger")
        return redirect(request.referrer or url_for('website_public.root_page'))

    submission_data = request.form.to_dict()

    # normalize + enrich (helps email + analytics)
    submission_data["form_id"] = (
        submission_data.get("form_id") or "").strip().lower()
    submission_data["remote_ip"] = request.remote_addr or ""
    submission_data["timestamp"] = (submission_data.get(
        "timestamp") or "").strip() or datetime.datetime.utcnow().isoformat()
    submission_data["referrer"] = request.referrer or ""
    submission_data["page"] = request.path

    module_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = ensure_data_folder(module_dir)  # noqa: F405
    submission_manager = ContactFormSubmissionManager(data_dir)  # noqa: F405

    success = submission_manager.process_submission(submission_data)
    if success:
        flash("Your submission has been received.", "success")
    else:
        flash("Submission saved, but processing was unsuccessful. Check logs for details.", "warning")

    return redirect(request.referrer or url_for('website_public.root_page'))


@website_public_routes.route('/privacy-policy')
def privacy_policy():
    website_settings = get_website_settings()
    config = get_core_manifest()
    policy_md = website_settings.get(
        'privacy_policy', 'No privacy policy set.')
    policy_html = markdown.markdown(policy_md)
    return render_template(
        'public/policy_page.html',
        title="Privacy Policy",
        description="Read how we collect, use, and protect your personal data under UK GDPR and the Data Protection Act 2018.",
        keywords="privacy, GDPR, data protection, Sparrow ERP, policy",
        policy=policy_html,
        config=config,
        website_settings=website_settings
    )


@website_public_routes.route('/cookie-policy')
def cookie_policy():
    website_settings = get_website_settings()
    config = get_core_manifest()
    policy_md = website_settings.get('cookie_policy', 'No cookie policy set.')
    policy_html = markdown.markdown(policy_md)
    return render_template(
        'public/policy_page.html',
        title="Cookie Policy",
        description="Learn about our use of cookies and how you can control them on Sparrow ERP.",
        keywords="cookies, cookie policy, privacy, Sparrow ERP",
        policy=policy_html,
        config=config,
        website_settings=website_settings
    )


@website_public_routes.route('/static/<path:filename>')
def website_static(filename):
    static_dir = os.path.join(os.path.dirname(__file__), 'static')
    return send_from_directory(static_dir, filename)


# ----------------------------
# Admin blueprint + builder (unchanged except minor hygiene)
# ----------------------------

# Define the paths for configuration files (legacy; overwritten below)
PAGES_JSON_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', 'config', 'pages.json'))

admin_template_folder = os.path.join(os.path.dirname(__file__), 'templates')
print(f"[Website Admin] Admin template folder: {admin_template_folder}")

website_admin_routes = Blueprint(
    'website_admin_routes',
    __name__,
    url_prefix='/plugin/website_module',
    template_folder=admin_template_folder
)


def get_blueprint():
    return website_admin_routes


# Pages persistence
PAGES_JSON_PATH = os.path.join(os.path.dirname(__file__), "pages.json")


def load_pages():
    if not os.path.exists(PAGES_JSON_PATH):
        return []
    with open(PAGES_JSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_pages(pages):
    with open(PAGES_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(pages, f, indent=4)


# Helpers
def _module_dir():
    return os.path.dirname(os.path.abspath(__file__))


def _data_dir():
    return ensure_data_folder(_module_dir())  # noqa: F405


def _normalize_route(route_str: str) -> str:
    route_str = (route_str or '').strip()
    return '/' + route_str.lstrip('/')


def _load_page_by_route(store, route_str: str):
    pages_dir = os.path.join(store.pages_dir)
    if not os.path.isdir(pages_dir):
        return None
    norm = _normalize_route(route_str)
    for fname in os.listdir(pages_dir):
        if not fname.endswith('.json'):
            continue
        try:
            with open(os.path.join(pages_dir, fname), 'r', encoding='utf-8') as f:
                pj = json.load(f)
        except Exception:
            continue
        if pj.get('route') == norm:
            return pj
    return None


def _get_or_create_page(store, route_str: str):
    page = _load_page_by_route(store, route_str)
    if page:
        return page
    norm = _normalize_route(route_str)
    title = 'Home' if norm == '/' else norm.strip(
        '/').replace('-', ' ').replace('_', ' ').title()
    return store.create(title=title, route=norm)


# Builder UI
@website_admin_routes.route('/builder', methods=['GET'], endpoint='builder_ui_root')
def builder_ui_root():
    app_root = os.path.abspath(os.path.join(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))), '..'))
    plugins_dir = os.path.abspath(os.path.join(app_root, 'plugins'))
    plugin_manager = PluginManager(plugins_dir)
    core_manifest = plugin_manager.get_core_manifest()

    store = BuilderPageStore(_data_dir())  # noqa: F405
    registry = BlocksRegistry()  # noqa: F405
    page = _get_or_create_page(store, '/')
    return render_template(
        'builder/builder.html',
        title=f"Website Builder — {page.get('title', '/')}",
        page=page,
        blocks_registry=registry.safe_registry(),
        config=core_manifest
    )


@website_admin_routes.route('/builder/<path:page_route>', methods=['GET'], endpoint='builder_ui')
def builder_ui(page_route):
    app_root = os.path.abspath(os.path.join(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))), '..'))
    plugins_dir = os.path.abspath(os.path.join(app_root, 'plugins'))
    plugin_manager = PluginManager(plugins_dir)
    core_manifest = plugin_manager.get_core_manifest()

    store = BuilderPageStore(_data_dir())  # noqa: F405
    registry = BlocksRegistry()  # noqa: F405
    page = _get_or_create_page(store, page_route)
    return render_template(
        'builder/builder.html',
        title=f"Website Builder — {page.get('title', page_route)}",
        page=page,
        blocks_registry=registry.safe_registry(),
        config=core_manifest
    )


# Live preview (iframe)
@website_admin_routes.route('/builder/preview', methods=['GET'], endpoint='builder_preview_root')
@website_admin_routes.route('/builder/<path:page_route>/preview', methods=['GET'], endpoint='builder_preview')
def builder_preview(page_route: str = ''):
    """Preview live merged builder output with public HTML templates."""
    effective_route = '/' if not page_route else '/' + page_route.lstrip('/')

    app_root = os.path.abspath(
        os.path.join(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))), '..')
    )
    plugins_dir = os.path.abspath(os.path.join(app_root, 'plugins'))
    plugin_manager = PluginManager(plugins_dir)
    core_manifest = plugin_manager.get_core_manifest()

    store = BuilderPageStore(_data_dir())  # noqa: F405
    renderer = BuilderRenderer()  # noqa: F405

    # Builder page/state
    page = _get_or_create_page(store, effective_route)
    builder_html = renderer.render_page(page) or ''

    # Manual HTML render for public template
    pages = _load_pages_json()
    page_data = next((p for p in pages if (
        p.get('route') or '') == effective_route), None)

    manual_html = ''
    if page_data:
        public_dir = os.path.join(os.path.dirname(
            __file__), 'templates', 'public')
        tpl_file = 'index.html' if effective_route == '/' else f"{effective_route.strip('/')}.html"
        candidate = os.path.join(public_dir, tpl_file)

        # Debug diagnostics
        print(f"[Preview] effective_route={effective_route}")
        print(f"[Preview] public_dir={public_dir}")
        print(f"[Preview] tpl_file={tpl_file}")
        print(f"[Preview] candidate_path={candidate}")
        print(f"[Preview] candidate_exists={os.path.exists(candidate)}")

        try:
            files = [f for f in os.listdir(public_dir) if f.endswith('.html')]
            print(f"[Preview] public templates list={files}")
        except Exception as e:
            print(f"[Preview] listdir error for {public_dir}: {e}")

        try:
            manual_html = render_template(
                tpl_file,
                page_data=page_data,
                pages=pages,
                config=core_manifest,
                website_settings=get_website_settings()
            )
            print(f"[Preview] render_template OK for {tpl_file}")
        except Exception as e:
            print(
                f"[Preview] Manual template render failed for {tpl_file}: {e}")
            # Fallback: read file and render with Flask's environment so url_for works
            try:
                with open(candidate, 'r', encoding='utf-8') as f:
                    tpl_source = f.read()
                manual_html = render_template_string(
                    tpl_source,
                    page_data=page_data,
                    pages=pages,
                    config=core_manifest,
                    website_settings=get_website_settings()
                )
                print(
                    f"[Preview] Fallback render_template_string OK for {candidate}")
            except Exception as ee:
                print(
                    f"[Preview] Fallback render_template_string error for {candidate}: {ee}")

    # Merge policy
    merge_mode = (page.get('settings') or {}).get('merge_mode', 'augment')
    if not builder_html:
        final_render = manual_html
    else:
        if merge_mode == 'replace':
            final_render = builder_html
        elif merge_mode == 'prepend':
            final_render = (builder_html or '') + (manual_html or '')
        else:
            final_render = (manual_html or '') + (builder_html or '')

    html = render_template(
        'builder/preview_base.html',
        title=page.get('seo', {}).get('title') or page.get(
            'title') or effective_route,
        page=page,
        rendered=final_render,
        config=core_manifest,
        pages=pages
    )
    resp = make_response(html)
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    return resp


# ------------------------
# Save draft (by route)
# ------------------------

@website_admin_routes.route('/builder/<path:page_route>/save', methods=['POST'], endpoint='builder_save')
def builder_save(page_route):
    """Saves a builder page draft including nested blocks and validates them."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({'ok': False, 'error': 'Invalid JSON payload'}), 400

    store = BuilderPageStore(_data_dir())  # noqa: F405
    registry = BlocksRegistry()  # noqa: F405
    page = _get_or_create_page(store, page_route)

    def clean_block(blk):
        btype = blk.get('type')
        if not btype or not registry.exists(btype):
            return None

        merged = registry.normalize(btype, blk.get('props'))
        ok, err = registry.validate(btype, merged)
        if not ok:
            raise ValueError(f"{btype}: {err}")

        if btype == 'section':
            nested = []
            for child in blk.get('props', {}).get('blocks', []):
                c = clean_block(child)
                if c:
                    nested.append(c)
            return {'type': btype, 'props': {**merged, 'blocks': nested}}

        if btype == 'columns':
            cols = []
            for col in blk.get('props', {}).get('columns', []):
                cleaned_col = []
                for child in col:
                    c = clean_block(child)
                    if c:
                        cleaned_col.append(c)
                cols.append(cleaned_col)
            return {'type': btype, 'props': {**merged, 'columns': cols}}

        return {'type': btype, 'props': merged}

    try:
        cleaned_blocks = []
        for blk in payload.get('blocks', []):
            c = clean_block(blk)
            if c:
                cleaned_blocks.append(c)
    except ValueError as e:
        return jsonify({'ok': False, 'error': str(e)}), 400

    page['blocks'] = cleaned_blocks
    page['vars'] = payload.get('vars', page.get('vars', {}))
    page['seo'] = payload.get('seo', page.get('seo', {}))
    page['draft'] = True
    page['version'] = int(page.get('version', 0)) + 1
    page['updated_at'] = datetime.datetime.utcnow().isoformat()
    store.save(page)
    return jsonify({'ok': True, 'version': page['version']})


# ------------------------
# Publish (by route)
# ------------------------

@website_admin_routes.route('/builder/<path:page_route>/publish', methods=['POST'], endpoint='builder_publish')
def builder_publish(page_route):
    """Publishes the current draft. Auto-creates page if needed."""
    store = BuilderPageStore(_data_dir())  # noqa: F405
    page = _get_or_create_page(store, page_route)
    page['draft'] = False
    page['published_at'] = datetime.datetime.utcnow().isoformat()
    page['updated_at'] = datetime.datetime.utcnow().isoformat()
    store.save(page)
    return jsonify({'ok': True})


# ------------------------
# Create page (explicit route)
# ------------------------

@website_admin_routes.route('/builder/create', methods=['POST'], endpoint='builder_create_page')
def builder_create_page():
    """Creates a new builder-managed page and returns canonical builder URL."""
    data = request.get_json(silent=True) or request.form
    title = (data.get('title') or '').strip()
    route = (data.get('route') or '').strip()
    if not title or not route or not route.startswith('/'):
        return jsonify({'ok': False, 'error': "Provide title and a route starting with '/'"}), 400

    store = BuilderPageStore(_data_dir())  # noqa: F405

    existing = _load_page_by_route(store, route)
    if existing:
        return jsonify({
            'ok': True,
            'builder_url': url_for('website_admin_routes.builder_ui', page_route=route.lstrip('/'))
        })

    store.create(title=title, route=route)
    return jsonify({
        'ok': True,
        'builder_url': url_for('website_admin_routes.builder_ui', page_route=route.lstrip('/'))
    })


# ------------------------
# Blocks registry (client-safe)
# ------------------------

@website_admin_routes.route('/builder/blocks/registry', methods=['GET'], endpoint='builder_blocks_registry')
def builder_blocks_registry():
    """Returns safe block metadata (label, icon, defaults, schema)."""
    return jsonify(BlocksRegistry().safe_registry())  # noqa: F405


# ------------------------
# Admin dashboard
# ------------------------

@website_admin_routes.route('/', methods=['GET'])
def admin_index():
    """
    Website Admin Dashboard with time-range filtering, deltas, ordered charts,
    popular pages with real % change, and country breakdown.
    """
    module_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = ensure_data_folder(module_dir)  # noqa: F405
    analytics_manager = AnalyticsManager(data_dir)  # noqa: F405

    valid_periods = {
        "today": "Today",
        "weekly": "Last 7 days",
        "monthly": "Last 30 days",
        "year": "Last 12 months",
        "alltime": "All time",
    }

    period = request.args.get("period", "alltime").lower()
    if period not in valid_periods:
        period = "alltime"

    current_range = analytics_manager.get_timerange(period)
    prev_range = analytics_manager.get_previous_timerange(period)

    if current_range != (None, None):
        total_views_current = sum(
            1 for _ in analytics_manager._iter_views(current_range))
    else:
        total_views_current = len(analytics_manager.get_page_views())

    if prev_range != (None, None):
        total_views_prev = sum(
            1 for _ in analytics_manager._iter_views(prev_range))
    else:
        total_views_prev = 0

    views_delta = None
    if total_views_prev:
        views_delta = round(
            (total_views_current - total_views_prev) * 100.0 / total_views_prev, 1)

    views_by_hour = analytics_manager.get_views_by_hour(current_range)
    views_by_weekday = analytics_manager.get_views_by_weekday(current_range)

    popular_now = analytics_manager.get_popular_pages(time_range=current_range)
    prev_dict = dict(analytics_manager.get_popular_pages(
        time_range=prev_range)) if prev_range != (None, None) else {}

    popular_pages_detailed = []
    for page, views in popular_now:
        prev = prev_dict.get(page, 0)
        change = round(((views - prev) * 100.0 / prev), 1) if prev else None
        popular_pages_detailed.append({
            "page": page,
            "views": views,
            "change": change,
        })

    requests_by_country = analytics_manager.get_requests_by_country(
        current_range)
    top_countries = sorted(requests_by_country.items(),
                           key=lambda x: x[1], reverse=True)[:10]
    total_country_views = sum(requests_by_country.values()) or 0

    # Plugin manifest (path-safe)
    plugin_manager = _get_plugin_manager()
    core_manifest = plugin_manager.get_core_manifest()

    analytics_data = {
        "total_views": total_views_current,
        "views_delta": views_delta,
        "views_by_hour": views_by_hour,
        "views_by_weekday": views_by_weekday,
        "popular_pages_detailed": popular_pages_detailed,
        "requests_by_country": requests_by_country,
        "top_countries": top_countries,
        "total_country_views": total_country_views,
        "current_period": period,
        "period_label": valid_periods[period],
    }

    return render_template(
        "admin/index.html",
        config=core_manifest,
        title="Website Admin Dashboard",
        analytics=analytics_data,
    )


@website_admin_routes.route('/contact-config', methods=['GET', 'POST'])
def contact_config():
    from .objects import ContactFormConfigManager  # noqa: F401
    MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
    config_manager = ContactFormConfigManager(MODULE_DIR)

    if request.method == 'POST' and request.form.get("action") == "delete":
        form_id = request.form.get("delete_form_id", "").strip()
        if form_id:
            current_config = config_manager.get_configuration()
            if form_id in current_config:
                del current_config[form_id]
                config_manager.save_config(current_config)
                return "Deleted", 200
            return "Not Found", 404
        return "Missing Form ID", 400

    if request.method == 'POST' and request.headers.get("X-Requested-With") == "XMLHttpRequest":
        form_id = request.form.get("form_id", "").strip()
        recipient = request.form.get("recipient", "").strip()
        subject = request.form.get("subject", "").strip()
        if not form_id or not recipient or not subject:
            return "Missing fields", 400
        config_manager.update_configuration(form_id, recipient, subject)
        return "Success", 200

    if request.method == 'POST':
        form_id = request.form.get("form_id", "").strip()
        recipient = request.form.get("recipient", "").strip()
        subject = request.form.get("subject", "").strip()
        if not form_id or not recipient or not subject:
            flash("All fields are required.", "danger")
        else:
            current_config = config_manager.get_configuration()
            if form_id in current_config:
                flash(
                    f"Configuration for form '{form_id}' already exists. Use inline editing to modify it.",
                    "warning"
                )
            else:
                current_config[form_id] = {
                    "recipient": recipient, "subject": subject}
                config_manager.save_config(current_config)
                flash(
                    f"Configuration for form '{form_id}' added successfully.", "success")
        return redirect(url_for('website_admin_routes.contact_config'))

    core_manifest = get_core_manifest()
    current_config = config_manager.get_configuration()
    return render_template("admin/contact_config.html", config=core_manifest, contact_config=current_config)

# ----------------------------
# Static helpers (admin)
# ----------------------------


PAGES_FILE = os.path.join(os.path.dirname(__file__), 'pages.json')
TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), 'templates', 'public')


def write_html_file(file_path, page_data):
    """
    Generate a basic HTML file with meta information.
    - Creates parent directories if they don't exist.
    - Writes either the "index.html" content or a default fallback for other pages.
    """
    path_obj = Path(file_path).resolve()
    path_obj.parent.mkdir(parents=True, exist_ok=True)

    if path_obj.name == "index.html":
        content = f"""{{% extends "base.html" %}}

{{% block title %}}{page_data['meta']['title']}{{% endblock %}}

{{% block content %}}
<!-- Main Content Section -->
<section class="container text-center mt-5">
    <h1 class="display-4">{page_data['title']}</h1>
    <p class="lead">{page_data['meta']['description']}</p>
    <!-- Get Started Button -->
    <button type="button" class="btn btn-primary btn-lg" data-mdb-toggle="modal" data-mdb-target="#getStartedModal">
        Get Started
    </button>
</section>

<!-- Features Section -->
<section class="features container text-center mt-5">
    <div class="row">
        <div class="col-md-4 mb-4">
            <div class="feature-icon mb-3">
                <i class="bi bi-gear" style="font-size: 2rem;"></i>
            </div>
            <h5>Modular Design</h5>
            <p>Extend functionality seamlessly with plug-and-play modules.</p>
        </div>
        <div class="col-md-4 mb-4">
            <div class="feature-icon mb-3">
                <i class="bi bi-lightning" style="font-size: 2rem;"></i>
            </div>
            <h5>Fast and Flexible</h5>
            <p>Built on Flask for rapid, lightweight web development.</p>
        </div>
        <div class="col-md-4 mb-4">
            <div class="feature-icon mb-3">
                <i class="bi bi-code-slash" style="font-size: 2rem;"></i>
            </div>
            <h5>Developer-Friendly</h5>
            <p>Write clean, extendable code with easy integration.</p>
        </div>
    </div>
</section>

<!-- Modal for Get Started -->
<div class="modal fade" id="getStartedModal" tabindex="-1" aria-labelledby="getStartedModalLabel" aria-hidden="true">
    <div class="modal-dialog">
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title" id="getStartedModalLabel">Get Started with Sparrow ERP</h5>
                <button type="button" class="btn-close" data-mdb-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
                <p>To create and edit your website, log in to the admin portal at:</p>
                <a href="http://localhost:82/"><p><strong>http://localhost:82/</strong></p></a>
                <p>Enhance your frontend by installing and activating additional modules.</p>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-secondary" data-mdb-dismiss="modal">Close</button>
            </div>
        </div>
    </div>
</div>
{{% endblock %}}
"""
    else:
        content = """
{% extends "base.html" %}

{% block title %}Home - Sparrow ERP{% endblock %}

{% block content %}
<!-- Hero Section -->
<section class="text-center py-5">
    <div class="container">
        <h1 class="display-3 fw-bold">{{ page_data.meta.title }}</h1>
        <p class="lead">{{ page_data.meta.description or "Start building your website with Sparrow ERP's modular framework." }}</p>
        <!-- Get Started Button -->
        <button type="button" class="btn btn-primary btn-lg" data-mdb-toggle="modal" data-mdb-target="#getStartedModal">
            Get Started
        </button>
    </div>
</section>

<!-- Modal for Get Started -->
<div class="modal fade" id="getStartedModal" tabindex="-1" aria-labelledby="getStartedModalLabel" aria-hidden="true">
    <div class="modal-dialog">
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title" id="getStartedModalLabel">Get Started with Sparrow ERP</h5>
                <button type="button" class="btn-close" data-mdb-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
                <p>Get into your Page Manager and start editing to create your perfect website!</p>
                <p>Access the admin portal at:</p>
                <a href="http://localhost:82/"><strong>http://localhost:82/</strong></a>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-secondary" data-mdb-dismiss="modal">Close</button>
            </div>
        </div>
    </div>
</div>
{% endblock %}
"""
    path_obj.write_text(content, encoding='utf-8')
    print(f"HTML file created/updated: {path_obj}")


@website_admin_routes.route('/edit_base', methods=['GET', 'POST'])
def edit_base_html():
    """
    Allows editing of the public base.html file.
    """
    base_html_path = os.path.join(os.path.dirname(
        __file__), 'templates', 'public', 'base.html')
    print(f"Loading base.html from: {base_html_path}")

    base_content = ""
    if os.path.exists(base_html_path):
        with open(base_html_path, 'r', encoding='utf-8') as f:
            base_content = f.read()
    else:
        print("base.html file not found!")

    if request.method == 'POST':
        updated_content = request.form['base_content']
        with open(base_html_path, 'w', encoding='utf-8') as f:
            f.write(updated_content)
        flash('Base.html updated successfully!', 'success')
        return redirect(url_for('website_admin_routes.page_manager'))

    pages = load_pages()
    return render_template('admin/page_manager.html', base_content=base_content, pages=pages)


@website_admin_routes.route('/pages', methods=['GET', 'POST'])
def page_manager():
    pages_path = os.path.join(os.path.dirname(__file__), 'pages.json')
    templates_dir = os.path.join(
        os.path.dirname(__file__), 'templates', 'public')
    base_html_path = os.path.join(templates_dir, 'base.html')

    core_manifest = get_core_manifest()

    # Ensure base.html exists
    if not os.path.exists(base_html_path):
        with open(base_html_path, 'w', encoding='utf-8') as f:
            f.write("""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ site_settings.company_name or 'Sparrow ERP' }}</title>
    <meta name="description" content="{{ page_data.meta.description if page_data.meta else '' }}">
    <meta name="keywords" content="{{ ', '.join(page_data.meta.keywords) if page_data.meta else '' }}">

    <link href="https://cdnjs.cloudflare.com/ajax/libs/mdb-ui-kit/6.4.0/mdb.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.5/font/bootstrap-icons.css">

    {% if theme_settings.custom_css_path %}
        <link rel="stylesheet" href="{{ url_for('static', filename=theme_settings.custom_css_path) }}">
    {% else %}
        <link rel="stylesheet" href="{{ url_for('static', filename='css/' + theme_settings.theme + '.css') }}">
    {% endif %}
</head>
<body>
    <nav class="navbar navbar-expand-lg navbar-light bg-light">
      <div class="container">
        {% if site_settings.branding == 'logo' and site_settings.logo_path %}
            <a class="navbar-brand" href="/">
                <img src="{{ url_for('static', filename=site_settings.logo_path) }}" alt="Logo" height="40">
            </a>
        {% else %}
            <a class="navbar-brand" href="/">{{ site_settings.company_name or 'Sparrow ERP' }}</a>
        {% endif %}
        <button class="navbar-toggler" type="button" data-mdb-toggle="collapse" data-mdb-target="#navbarNav"
          aria-controls="navbarNav" aria-expanded="false" aria-label="Toggle navigation">
          <i class="fas fa-bars"></i>
        </button>
        <div class="collapse navbar-collapse" id="navbarNav">
          <ul class="navbar-nav ms-auto">
            {% for page in pages %}
                {% if page.header %}
                    <li class="nav-item">
                        <a class="nav-link" href="{{ url_for('website_public.custom_page', page_route=page.route.strip('/')) }}">
                            {{ page.title }}
                        </a>
                    </li>
                {% endif %}
            {% endfor %}
            <li class="nav-item">
                <a class="nav-link" href="{{ url_for('routes.logout') }}">Logout</a>
            </li>
          </ul>
        </div>
      </div>
    </nav>

    <div class="container mt-5">
        {% block content %}{% endblock %}
    </div>

    <footer class="bg-light text-center text-lg-start mt-5">
      <div class="container p-4">
        <div class="row">
          <div class="col-lg-6 col-md-12 mb-4 mb-md-0">
            <h5 class="text-uppercase">Powered by Sparrow ERP</h5>
            <p>Sparrow ERP offers powerful website and e-commerce capabilities, seamlessly integrated with your business.</p>
          </div>
          <div class="col-lg-6 col-md-12">
            <h5 class="text-uppercase">Quick Links</h5>
            <ul class="list-unstyled mb-0">
                {% for page in pages %}
                    {% if page.footer %}
                        <li>
                            <a href="{{ url_for('website_public.custom_page', page_route=page.route.strip('/')) }}" class="text-dark">
                                {{ page.title }}
                            </a>
                        </li>
                    {% endif %}
                {% endfor %}
            </ul>
          </div>
        </div>
      </div>
      <div class="text-center p-3 bg-dark text-white">
        CopyRight 2025 <strong>{{ site_settings.company_name or 'Sparrow ERP' }}</strong>. All Rights Reserved. | Powered by Sparrow ERP
      </div>
    </footer>

    <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/mdb-ui-kit/6.4.0/mdb.min.js"></script>
</body>
</html>""")

    # Ensure pages.json exists with a default Home page
    if not os.path.exists(pages_path):
        default_pages = [
            {
                "title": "Home",
                "route": "/",
                "header": True,
                "footer": True,
                "meta": {
                    "title": "Sparrow ERP - Build with Ease",
                    "description": "Discover the power of Sparrow ERP's modular, Flask-based architecture.",
                    "keywords": ["Sparrow ERP", "modular development", "Flask", "ERP system"]
                }
            }
        ]
        with open(pages_path, 'w', encoding='utf-8') as f:
            json.dump(default_pages, f, indent=4)
        print("Default pages.json created.")

    # Check if index.html exists; create it only if missing
    index_html_path = os.path.join(templates_dir, "index.html")
    if not os.path.exists(index_html_path):
        with open(pages_path, 'r', encoding='utf-8') as f:
            pages = json.load(f)
        home_page = next((p for p in pages if p.get('route') == '/'), None)
        if home_page:
            write_html_file(index_html_path, home_page)
            print("Default index.html created.")

    # Handle POST requests
    if request.method == 'POST':
        if 'add_page' in request.form:
            title = request.form['title']
            route = request.form['route'].strip('/')
            file_name = 'index.html' if route == '' else f"{route}.html"
            html_path = os.path.join(templates_dir, file_name)

            new_page = {
                "title": title,
                "route": f"/{route}" if route else "/",
                "header": 'header' in request.form,
                "footer": 'footer' in request.form,
                "meta": {"title": title, "description": "", "keywords": []}
            }

            with open(pages_path, 'r+', encoding='utf-8') as f:
                pages = json.load(f)
                pages.append(new_page)
                f.seek(0)
                json.dump(pages, f, indent=4)

            write_html_file(html_path, new_page)

        elif 'edit_page' in request.form:
            index = int(request.form['index'])
            with open(pages_path, 'r+', encoding='utf-8') as f:
                pages = json.load(f)
                old_route = (pages[index].get('route') or '').strip('/')
                old_file_name = 'index.html' if old_route == '' else f"{old_route}.html"
                old_html_path = os.path.join(templates_dir, old_file_name)

                route = request.form['route'].strip('/')
                new_file_name = 'index.html' if route == '' else f"{route}.html"
                new_html_path = os.path.join(templates_dir, new_file_name)

                pages[index]['title'] = request.form['title']
                pages[index]['route'] = f"/{route}" if route else "/"
                pages[index]['header'] = 'header' in request.form
                pages[index]['footer'] = 'footer' in request.form
                pages[index]['meta'] = {
                    'title': request.form['meta_title'],
                    'description': request.form['meta_description'],
                    'keywords': [k.strip() for k in request.form['meta_keywords'].split(',') if k.strip()]
                }

                if old_html_path != new_html_path:
                    if os.path.exists(old_html_path):
                        os.rename(old_html_path, new_html_path)
                    else:
                        write_html_file(new_html_path, pages[index])

                f.seek(0)
                f.truncate()
                json.dump(pages, f, indent=4)

        elif 'edit_content' in request.form:
            index = int(request.form['index'])
            content = request.form['content']
            with open(pages_path, 'r', encoding='utf-8') as f:
                pages = json.load(f)
            route = (pages[index].get('route') or '').strip('/')
            file_name = 'index.html' if route == '' else f"{route}.html"
            html_path = os.path.join(templates_dir, file_name)
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(content)

        elif 'delete_page' in request.form:
            index = int(request.form['index'])
            with open(pages_path, 'r+', encoding='utf-8') as f:
                pages = json.load(f)
                deleted_page = pages.pop(index)
                f.seek(0)
                f.truncate()
                json.dump(pages, f, indent=4)

            route = (deleted_page.get('route') or '').strip('/')
            file_name = 'index.html' if route == '' else f"{route}.html"
            html_path = os.path.join(templates_dir, file_name)
            if os.path.exists(html_path):
                os.remove(html_path)

        elif 'edit_base' in request.form:
            updated_content = request.form['base_content']
            with open(base_html_path, 'w', encoding='utf-8') as f:
                f.write(updated_content)

    # Load pages and base.html content
    base_content = ""
    if os.path.exists(base_html_path):
        with open(base_html_path, 'r', encoding='utf-8') as f:
            base_content = f.read()

    if os.path.exists(pages_path):
        with open(pages_path, 'r', encoding='utf-8') as f:
            pages = json.load(f)
            for page in pages:
                route = (page.get('route') or '').strip('/')
                file_name = 'index.html' if route == '' else f"{route}.html"
                html_path = os.path.join(templates_dir, file_name)
                if os.path.exists(html_path):
                    with open(html_path, 'r', encoding='utf-8') as html_file:
                        page['content'] = html_file.read()
                else:
                    page['content'] = ""
    else:
        pages = []

    return render_template(
        'admin/page_manager.html',
        pages=pages,
        config=core_manifest,
        base_content=base_content,
        public_url="http://localhost:80"
    )


@website_admin_routes.route('/pages/meta/<int:page_index>', methods=['POST'])
def edit_meta(page_index):
    pages_path = os.path.join(os.path.dirname(__file__), 'pages.json')
    templates_dir = os.path.join(
        os.path.dirname(__file__), '../templates/public')

    with open(pages_path, 'r', encoding='utf-8') as f:
        pages = json.load(f)

    meta_title = request.form.get('meta_title', '').strip()
    meta_description = request.form.get('meta_description', '').strip()
    meta_keywords = request.form.get('meta_keywords', '').split(',')

    pages[page_index]['meta'] = {
        'title': meta_title,
        'description': meta_description,
        'keywords': [k.strip() for k in meta_keywords if k.strip()]
    }

    page = pages[page_index]
    html_path = os.path.join(templates_dir, f"{page['route'].strip('/')}.html")
    write_html_file(html_path, page)

    with open(pages_path, 'w', encoding='utf-8') as f:
        json.dump(pages, f, indent=4)

    flash('Meta content updated successfully!', 'success')
    return redirect(url_for('website_admin_routes.page_manager'))


@website_admin_routes.route('/website_module_static/<path:filename>')
def website_module_static(filename):
    static_dir = os.path.join(os.path.dirname(__file__), 'static')
    return send_from_directory(static_dir, filename)


@website_public_routes.route('/favicon.ico')
def public_favicon():
    """
    Serve a favicon at a stable URL for browsers + Google.
    1) Prefer static/favicon.ico
    2) Else fallback to settings['favicon_path']
    3) Else 404
    """
    static_dir = os.path.join(os.path.dirname(__file__), 'static')

    ico_path = os.path.join(static_dir, 'favicon.ico')
    if os.path.exists(ico_path):
        return send_from_directory(static_dir, 'favicon.ico')

    try:
        settings = get_website_settings() or {}
    except Exception:
        settings = {}

    fav = (settings.get('favicon_path') or '').strip()
    if fav:
        fav_abs = os.path.join(static_dir, fav)
        if os.path.exists(fav_abs):
            guessed_type = mimetypes.guess_type(fav_abs)[0] or 'image/x-icon'
            resp = send_from_directory(static_dir, fav)
            resp.headers['Content-Type'] = guessed_type
            return resp

    return Response("Not found", status=404)


# ----------------------------
# Policy generators (unchanged)
# ----------------------------

def generate_privacy_policy(company, website, email, today, settings):
    lines = [
        f"## Privacy Policy for {company}",
        f"_Effective Date: {today}_",
        "",
        "## Introduction",
        f"Welcome to {company}! We are committed to protecting your privacy and complying with the UK General Data Protection Regulation (UK GDPR) and Data Protection Act 2018. This Privacy Policy explains how we collect, use, and protect your personal data when you use our website ({website}) or contact us.",
        "",
        "## 1. Information We Collect",
        "We may collect and process the following personal data:",
    ]
    if settings.get('has_contact_forms'):
        lines.append(
            "- **Contact Information:** Name, email address, phone number, and any other details you provide when contacting us.")
    if settings.get('has_comments'):
        lines.append(
            "- **Comments/Reviews:** Content and details you submit as comments or reviews.")
    if settings.get('has_ecommerce'):
        lines.append(
            "- **Order Information:** Data for processing orders and payments (address, payment details, etc.).")
    if settings.get('has_newsletter'):
        lines.append(
            "- **Newsletter/Marketing:** Your email address and preferences if you subscribe.")
    if settings.get('has_user_accounts'):
        lines.append(
            "- **User Accounts:** Information needed for account creation and login.")
    lines.append(
        "- **Usage Data:** IP address, browser type, device info, pages visited, and analytics data (see 'Cookies and Tracking').")
    lines.append("")
    lines.append("## 2. How We Use Your Information")
    lines.append("We use your information to:")
    lines.append("- Respond to your enquiries or requests")
    if settings.get('has_ecommerce'):
        lines.append("- Process and fulfil orders")
    if settings.get('has_newsletter'):
        lines.append(
            "- Send newsletters and marketing communications (with your consent)")
    if settings.get('has_user_accounts'):
        lines.append("- Manage user accounts and authentication")
    if settings.get('has_comments'):
        lines.append("- Display comments or reviews you submit")
    lines.append(
        "- Improve our website and services through analytics and feedback")
    lines.append("")
    lines.append("## 3. Lawful Basis for Processing")
    lines.append("We process your personal data where it is necessary for the performance of a contract, compliance with a legal obligation, or for our legitimate interests. For marketing, we rely on your consent, which you can withdraw at any time.")
    lines.append("")
    lines.append("## 4. Data Sharing and Disclosure")
    lines.append(
        "We do not sell your data. We may share your information with:")
    lines.append(
        "- Trusted service providers (e.g., website hosting, analytics, email delivery) who must keep your data confidential")
    lines.append("- Authorities, if required by law or to protect our rights")
    lines.append("")
    lines.append("## 5. International Data Transfers")
    lines.append("If your data is transferred outside the UK, we ensure appropriate safeguards are in place to protect it in accordance with UK data protection law.")
    lines.append("")
    lines.append("## 6. Data Security")
    lines.append("We use technical and organisational measures to protect your data, including HTTPS encryption and access controls. However, no system is completely secure.")
    lines.append("")
    lines.append("## 7. Cookies and Tracking Technologies")
    lines.append(
        "We use cookies and similar technologies to improve your experience and analyse usage. See our Cookie Policy for details.")
    if settings.get('uses_google_analytics'):
        lines.append("- We use Google Analytics for website usage analytics.")
    if settings.get('uses_facebook_pixel'):
        lines.append("- We use Facebook Pixel for marketing analytics.")
    if settings.get('has_social_sharing'):
        lines.append("- Social sharing features may set cookies.")
    lines.append("")
    lines.append("## 8. Your Rights")
    lines.append("You have the right to:")
    lines.append("- Access the personal data we hold about you")
    lines.append("- Request corrections to your data")
    lines.append(
        "- Request deletion of your data, subject to legal requirements")
    lines.append("- Object to or restrict processing")
    lines.append("- Withdraw consent for marketing at any time")
    lines.append(f"To exercise your rights, contact us at **{email}**.")
    lines.append("")
    lines.append("## 9. Changes to This Policy")
    lines.append(
        "We may update this Privacy Policy from time to time. Changes will be posted on this page and the effective date updated.")
    lines.append("")
    lines.append("## 10. Contact Us")
    lines.append(
        f"If you have any questions or concerns about this policy or your data, contact us at **{email}** or visit **{website}**.")
    return "\n".join(lines)


def generate_cookie_policy(company, website, email, today, settings):
    lines = [
        f"## Cookie Policy for {company}",
        f"_Effective Date: {today}_",
        "",
        "## Introduction",
        f"This Cookie Policy explains how {company} uses cookies and similar technologies on our website ({website}). By using our website, you agree to our use of cookies as described below.",
        "",
        "## 1. What Are Cookies?",
        "Cookies are small text files placed on your device to help websites function, improve user experience, and provide information to site owners.",
        "",
        "## 2. How We Use Cookies",
        "We use cookies to:",
        "- Ensure website functionality and security",
        "- Remember your preferences",
        "- Analyse website traffic and usage patterns",
    ]
    if settings.get('uses_google_analytics'):
        lines.append(
            "- Use analytics services (Google Analytics) to understand visitor behaviour")
    if settings.get('uses_facebook_pixel'):
        lines.append("- Use Facebook Pixel for marketing analytics")
    if settings.get('has_ecommerce'):
        lines.append("- Support ecommerce features (e.g., shopping cart)")
    if settings.get('has_social_sharing'):
        lines.append("- Enable social sharing features")
    lines.append("")
    lines.append("## 3. Types of Cookies We Use")
    lines.append(
        "- **Strictly Necessary Cookies:** Essential for website operation")
    lines.append(
        "- **Performance Cookies:** Collect information about how visitors use the site")
    lines.append(
        "- **Functionality Cookies:** Remember your settings and preferences")
    lines.append("")
    lines.append("## 4. Third-Party Cookies")
    lines.append("Some cookies may be set by third-party services such as analytics or embedded content. These are subject to their own privacy policies.")
    lines.append("")
    lines.append("## 5. Managing Cookies")
    lines.append(
        "You can control and delete cookies through your browser settings. Disabling cookies may affect website functionality. For more information, visit [www.aboutcookies.org](https://www.aboutcookies.org/).")
    lines.append("")
    lines.append("## 6. Changes to This Cookie Policy")
    lines.append(
        "We may update this Cookie Policy from time to time. Changes will be posted on this page with an updated effective date.")
    lines.append("")
    lines.append("## 7. Contact Us")
    lines.append(
        f"If you have any questions about our use of cookies, contact us at **{email}** or visit **{website}**.")
    return "\n".join(lines)


# --- Settings route ---

@website_admin_routes.route('/settings', methods=['GET', 'POST'])
def website_settings():
    manifest_path = os.path.join(os.path.dirname(__file__), 'manifest.json')
    manifest = {}
    if os.path.exists(manifest_path):
        with open(manifest_path, encoding='utf-8') as f:
            manifest = json.load(f)
    settings = manifest.get('settings', {}) or {}

    generator_fields = [
        'has_ecommerce', 'has_newsletter', 'uses_google_analytics', 'uses_facebook_pixel',
        'has_contact_forms', 'has_user_accounts', 'has_comments', 'has_social_sharing'
    ]
    for f in generator_fields:
        settings.setdefault(f, False)

    if request.method == 'POST':
        # --- File uploads ---
        favicon = request.files.get('favicon')
        if favicon and favicon.filename:
            favicon_path = os.path.join('static', 'favicon.ico')
            favicon.save(os.path.join(os.path.dirname(__file__), favicon_path))
            settings['favicon_path'] = 'favicon.ico'

        og_image = request.files.get('default_og_image')
        if og_image and og_image.filename:
            og_path = os.path.join('static', og_image.filename)
            og_image.save(os.path.join(os.path.dirname(__file__), og_path))
            settings['default_og_image'] = og_image.filename

        # --- Main fields ---
        settings['analytics_code'] = request.form.get('analytics_code', '')
        settings['schema_json'] = request.form.get('schema_json', '')

        social_keys = [
            'facebook_url', 'instagram_url', 'linkedin_url', 'twitter_url', 'youtube_url', 'tiktok_url', 'pinterest_url',
            'whatsapp_url', 'threads_url', 'reddit_url', 'snapchat_url', 'telegram_url', 'discord_url', 'tumblr_url',
            'github_url', 'medium_url', 'vimeo_url', 'dribbble_url', 'behance_url', 'soundcloud_url', 'slack_url', 'mastodon_url'
        ]
        for key in social_keys:
            settings[key] = request.form.get(key, '')

        # GDPR/cookie
        settings['privacy_policy'] = request.form.get('privacy_policy', '')
        settings['cookie_policy'] = request.form.get('cookie_policy', '')
        settings['cookie_bar_text'] = request.form.get('cookie_bar_text', '')
        settings['cookie_bar_colors'] = request.form.get(
            'cookie_bar_colors', '#333')
        settings['require_cookie_consent'] = bool(
            request.form.get('require_cookie_consent'))

        for f in generator_fields:
            settings[f] = bool(request.form.get(f))

        if request.form.get('generate_policies'):
            core_manifest = get_core_manifest()
            company = core_manifest.get('company_name', 'Your Company')
            website = core_manifest.get(
                'website_url', 'https://yourwebsite.com')
            email = core_manifest.get('contact_email', 'info@yourcompany.com')
            today = date.today().strftime('%d/%m/%Y')

            settings['privacy_policy'] = generate_privacy_policy(
                company, website, email, today, settings)
            settings['cookie_policy'] = generate_cookie_policy(
                company, website, email, today, settings)
            flash(
                'Policies generated. Please review placeholders and edit as needed.', 'info')
        else:
            flash('Website settings updated!', 'success')

        manifest['settings'] = settings
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=4)
        return redirect(url_for('website_admin_routes.website_settings'))

    return render_template('admin/website_settings.html', settings=settings, config=get_core_manifest())


# Admin route registration function
def register_admin_routes(app):
    """
    Function to register the admin routes for the Website Module.
    This will be called dynamically by the Core Module.
    """
    app.register_blueprint(website_admin_routes)
