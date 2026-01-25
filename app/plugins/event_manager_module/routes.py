from datetime import datetime, date
import os
from flask import (
    Blueprint, request, send_from_directory,
    render_template, redirect, url_for, flash
)
from app.objects import PluginManager
from .objects import EventService

# Blueprints
internal_template_folder = os.path.join(os.path.dirname(__file__), 'templates')
internal_bp = Blueprint("internal_event_manager", __name__,
                        url_prefix="/plugin/event_manager_module",
                        template_folder=internal_template_folder)
public_bp = Blueprint("public_event_manager", __name__,
                      url_prefix="/events",
                      template_folder=internal_template_folder)

plugin_manager = PluginManager(os.path.abspath('app/plugins'))
core_manifest = plugin_manager.get_core_manifest()


def get_menu_upload_dir():
    base = os.path.join(os.path.dirname(__file__), "static", "menus")
    os.makedirs(base, exist_ok=True)
    return base


def get_flyer_upload_dir():
    base = os.path.join(os.path.dirname(__file__), "static", "flyers")
    os.makedirs(base, exist_ok=True)
    return base


def get_event_kpis(events):
    today = date.today()

    def parse_date(val):
        if isinstance(val, date):
            return val
        try:
            return datetime.strptime(val, "%Y-%m-%d").date()
        except Exception:
            return None
    return {
        "upcoming_events": len([
            e for e in events
            if e.get("start_date") and parse_date(e.get("start_date")) and parse_date(e.get("start_date")) >= today
        ]),
        "public_events": len([e for e in events if e.get("is_public")]),
        "menu_events": len([e for e in events if e.get("food_menu_path")]),
    }


@internal_bp.route("/", methods=["GET", "POST"])
def events_list():
    if request.method == "POST":
        data = request.form.to_dict()
        is_edit = data.get("is_edit")
        event_id = data.get("event_id")

        # Category override
        if data.get("category") == "Other":
            other = data.get("category_other", "").strip()
            if other:
                data["category"] = other

        # File uploads
        file = request.files.get("food_menu")
        if file and file.filename:
            upload_dir = get_menu_upload_dir()
            filename = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{file.filename.replace(' ', '_')}"
            file.save(os.path.join(upload_dir, filename))
            data["food_menu_path"] = filename

        flyer_file = request.files.get("flyer")
        if flyer_file and flyer_file.filename:
            flyer_dir = get_flyer_upload_dir()
            flyer_filename = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{flyer_file.filename.replace(' ', '_')}"
            flyer_file.save(os.path.join(flyer_dir, flyer_filename))
            data["flyer_path"] = flyer_filename

        if is_edit and event_id:
            EventService.update_event(event_id, data)
            flash("Event updated.", "success")
        else:
            EventService.create_event(data)
            flash("Event created.", "success")
        return redirect(url_for("internal_event_manager.events_list"))

    # GET
    q = request.args.get("q")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    music_type = request.args.get("music_type")
    is_public = request.args.get("is_public")
    is_public = int(is_public) if is_public in ("0", "1") else None
    page = int(request.args.get("page") or 1)
    page_size = int(request.args.get("page_size") or 20)
    is_public_view = request.blueprint == "public_event_manager"
    if is_public_view:
        is_public = 1

    results = EventService.search_events(
        keyword=q,
        start_date=start_date,
        end_date=end_date,
        music_type=music_type,
        is_public=is_public,
        page=page,
        page_size=page_size
    )

    kpis = None
    if not is_public_view:
        all_events = EventService.get_all_events()
        kpis = get_event_kpis(all_events)

    template = "public/events_list.html" if is_public_view else "admin/events_list.html"
    return render_template(
        template,
        events=results["items"],
        total=results["total"],
        page=results["page"],
        page_size=results["page_size"],
        q=q,
        start_date=start_date,
        end_date=end_date,
        music_type=music_type,
        is_public=is_public,
        config=core_manifest,
        kpis=kpis
    )


@internal_bp.post("/delete/<int:event_id>")
def admin_event_delete(event_id):
    EventService.delete_event(event_id)
    flash("Event deleted.", "success")
    return redirect(url_for("internal_event_manager.events_list"))


@internal_bp.get("/details/<int:event_id>")
@public_bp.get("/details/<int:event_id>")
def event_detail(event_id):
    event = EventService.get_event_by_id(event_id)
    is_public_view = request.blueprint == "public_event_manager"
    if not event or (is_public_view and not event.get("is_public")):
        flash("Event not found.", "error")
        return redirect(url_for(f"{request.blueprint}.events_list"))
    template = "public/event_detail.html" if is_public_view else "admin/event_detail.html"
    return render_template(template, event=event, config=core_manifest)


@internal_bp.get("/menu/<filename>")
@public_bp.get("/menu/<filename>")
def menu_download(filename):
    menu_dir = os.path.join(os.path.dirname(__file__), "static", "menus")
    file_path = os.path.join(menu_dir, filename)
    if not os.path.isfile(file_path):
        flash("Menu file not found.", "error")
        return redirect(url_for(f"{request.blueprint}.events_list"))
    return send_from_directory(menu_dir, filename, as_attachment=True)


@internal_bp.get("/flyer/<filename>")
@public_bp.get("/flyer/<filename>")
def flyer_download(filename):
    flyer_dir = os.path.join(os.path.dirname(__file__), "static", "flyers")
    file_path = os.path.join(flyer_dir, filename)
    if not os.path.isfile(file_path):
        flash("Flyer file not found.", "error")
        return redirect(url_for(f"{request.blueprint}.events_list"))
    return send_from_directory(flyer_dir, filename, as_attachment=True)


@public_bp.route("/", methods=["GET"])
def events_list():
    # GET: Listing/Search for public events only
    q = request.args.get("q")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    music_type = request.args.get("music_type")
    page = int(request.args.get("page") or 1)
    page_size = int(request.args.get("page_size") or 20)
    is_public = 1  # Only show public events

    results = EventService.search_events(
        keyword=q,
        start_date=start_date,
        end_date=end_date,
        music_type=music_type,
        is_public=is_public,
        page=page,
        page_size=page_size
    )

    return render_template(
        "public/events_list.html",
        events=results["items"],
        total=results["total"],
        page=results["page"],
        page_size=results["page_size"],
        q=q,
        start_date=start_date,
        end_date=end_date,
        music_type=music_type,
        config=core_manifest
    )


def get_blueprint():
    return internal_bp


def get_public_blueprint():
    return public_bp
