import logging
import re
from app.objects import get_db_connection


class ArticleService:
    """
    Production-ready service for the News/Blog module.

    V1 upgrades included:
    - Public keyword search support in get_public_articles(keyword=...) + q alias
    - Safer SQL aliasing for article_tags join (avoid `at` alias)
    - Optional: attach taxonomy to public list items (kept OFF by default for speed)
    - Defensive input coercion + consistent behavior

    Additional hardening included in this version:
    - Page/page_size normalization + sensible bounds (prevents negative offsets / abuse)
    - Consistent int casting for totals
    - Optional attach_taxonomy flag for public list (default False)
    - Category/tag slug normalization (strip)
    """

    # =============================================================================
    # Slug helpers
    # =============================================================================

    @staticmethod
    def _slugify(text: str) -> str:
        text = (text or "").strip().lower()
        text = re.sub(r"[^a-z0-9]+", "-", text)
        text = re.sub(r"-{2,}", "-", text).strip("-")
        return text or "item"

    @staticmethod
    def _coerce_status(val):
        v = (val or "draft").strip().lower()
        return v if v in ("draft", "published") else "draft"

    @staticmethod
    def _coerce_int_list(values):
        if not values:
            return []
        if isinstance(values, (list, tuple)):
            raw = values
        else:
            raw = [values]
        out = []
        for v in raw:
            try:
                out.append(int(v))
            except Exception:
                continue
        # de-dupe keep order
        seen = set()
        uniq = []
        for x in out:
            if x not in seen:
                uniq.append(x)
                seen.add(x)
        return uniq

    @staticmethod
    def _normalize_page(page, default=1):
        try:
            p = int(page or default)
        except Exception:
            p = default
        if p < 1:
            p = 1
        return p

    @staticmethod
    def _normalize_page_size(page_size, default=10, max_size=100):
        try:
            ps = int(page_size or default)
        except Exception:
            ps = default
        if ps < 1:
            ps = default
        if ps > max_size:
            ps = max_size
        return ps

    @staticmethod
    def _ensure_unique_article_slug(slug, exclude_id=None):
        """
        Canonical URL is /news/{slug}, so slug must be globally unique.
        """
        base = ArticleService._slugify(slug)
        candidate = base
        i = 2

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            while True:
                if exclude_id:
                    cur.execute(
                        "SELECT id FROM articles WHERE slug=%s AND id<>%s LIMIT 1",
                        (candidate, int(exclude_id)),
                    )
                else:
                    cur.execute(
                        "SELECT id FROM articles WHERE slug=%s LIMIT 1",
                        (candidate,),
                    )
                row = cur.fetchone()
                if not row:
                    return candidate
                candidate = f"{base}-{i}"
                i += 1
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # =============================================================================
    # Admin KPIs (SQL, fast)
    # =============================================================================

    @staticmethod
    def get_article_kpis():
        """
        Fast KPI counts without loading all rows.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT
                  COUNT(*) AS total_articles,
                  SUM(CASE WHEN status='published' THEN 1 ELSE 0 END) AS published_articles,
                  SUM(CASE WHEN status<>'published' THEN 1 ELSE 0 END) AS draft_articles
                FROM articles
                """
            )
            row = cur.fetchone() or {}
            return {
                "total_articles": int(row.get("total_articles") or 0),
                "published_articles": int(row.get("published_articles") or 0),
                "draft_articles": int(row.get("draft_articles") or 0),
            }
        except Exception as e:
            logging.exception("Error computing article KPIs: %s", e)
            return {"total_articles": 0, "published_articles": 0, "draft_articles": 0}
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # =============================================================================
    # Categories CRUD
    # =============================================================================

    @staticmethod
    def get_all_categories():
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM categories ORDER BY sort_order ASC, name ASC")
            return cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def get_category_by_id(category_id):
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM categories WHERE id=%s",
                        (int(category_id),))
            return cur.fetchone()
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def get_category_by_slug(slug):
        slug = (slug or "").strip()
        if not slug:
            return None
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT * FROM categories WHERE slug=%s LIMIT 1", (slug,))
            return cur.fetchone()
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def create_category(name, slug=None, description=None, sort_order=0):
        name = (name or "").strip()
        if not name:
            return None
        slug = ArticleService._slugify(slug or name)

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "INSERT INTO categories (name, slug, description, sort_order) VALUES (%s, %s, %s, %s)",
                (name, slug, description, int(sort_order or 0)),
            )
            conn.commit()
            return ArticleService.get_category_by_id(cur.lastrowid)
        except Exception as e:
            logging.exception("Error creating category: %s", e)
            return None
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def update_category(category_id, name, slug=None, description=None, sort_order=0):
        category_id = int(category_id)
        name = (name or "").strip()
        if not name:
            return None
        slug = ArticleService._slugify(slug or name)

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "UPDATE categories SET name=%s, slug=%s, description=%s, sort_order=%s WHERE id=%s",
                (name, slug, description, int(sort_order or 0), category_id),
            )
            conn.commit()
            return ArticleService.get_category_by_id(category_id)
        except Exception as e:
            logging.exception("Error updating category: %s", e)
            return None
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def delete_category(category_id):
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM categories WHERE id=%s",
                        (int(category_id),))
            conn.commit()
        except Exception as e:
            logging.exception("Error deleting category: %s", e)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # =============================================================================
    # Tags CRUD
    # =============================================================================

    @staticmethod
    def get_all_tags():
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM tags ORDER BY name ASC")
            return cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def get_tag_by_id(tag_id):
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM tags WHERE id=%s", (int(tag_id),))
            return cur.fetchone()
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def get_tag_by_slug(slug):
        slug = (slug or "").strip()
        if not slug:
            return None
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM tags WHERE slug=%s LIMIT 1", (slug,))
            return cur.fetchone()
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def create_tag(name, slug=None):
        name = (name or "").strip()
        if not name:
            return None
        slug = ArticleService._slugify(slug or name)

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "INSERT INTO tags (name, slug) VALUES (%s, %s)", (name, slug))
            conn.commit()
            return ArticleService.get_tag_by_id(cur.lastrowid)
        except Exception as e:
            logging.exception("Error creating tag: %s", e)
            return None
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def update_tag(tag_id, name, slug=None):
        tag_id = int(tag_id)
        name = (name or "").strip()
        if not name:
            return None
        slug = ArticleService._slugify(slug or name)

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "UPDATE tags SET name=%s, slug=%s WHERE id=%s", (name, slug, tag_id))
            conn.commit()
            return ArticleService.get_tag_by_id(tag_id)
        except Exception as e:
            logging.exception("Error updating tag: %s", e)
            return None
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def delete_tag(tag_id):
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM tags WHERE id=%s", (int(tag_id),))
            conn.commit()
        except Exception as e:
            logging.exception("Error deleting tag: %s", e)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def get_or_create_tags_by_names(tag_names):
        """
        Accepts list of strings. Creates missing tags. Returns list of tag dicts.

        V1 hardening:
        - case-insensitive de-dupe
        - best-effort race safety: if insert fails due to unique slug, re-select
        """
        names = []
        for n in (tag_names or []):
            n = (n or "").strip()
            if n:
                names.append(n)

        # de-dupe case-insensitive
        seen = set()
        uniq = []
        for n in names:
            key = n.lower()
            if key not in seen:
                uniq.append(n)
                seen.add(key)

        if not uniq:
            return []

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            out = []
            for name in uniq:
                slug = ArticleService._slugify(name)

                cur.execute(
                    "SELECT * FROM tags WHERE slug=%s LIMIT 1", (slug,))
                existing = cur.fetchone()
                if existing:
                    out.append(existing)
                    continue

                try:
                    cur.execute(
                        "INSERT INTO tags (name, slug) VALUES (%s, %s)", (name, slug))
                    conn.commit()
                    out.append(ArticleService.get_tag_by_id(cur.lastrowid))
                except Exception:
                    # If another request created it first, fetch it now
                    conn.rollback()
                    cur.execute(
                        "SELECT * FROM tags WHERE slug=%s LIMIT 1", (slug,))
                    out.append(cur.fetchone())

            return [t for t in out if t]
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # =============================================================================
    # Article taxonomy helpers
    # =============================================================================

    @staticmethod
    def set_article_categories(article_id, category_ids, primary_category_id=None):
        """
        Replaces article categories with the provided list.
        If primary_category_id is provided and included, marks it as primary.
        If primary_category_id is not provided, first category becomes primary (if any).
        """
        article_id = int(article_id)
        category_ids = ArticleService._coerce_int_list(category_ids)

        primary_id = None
        if primary_category_id not in (None, "", 0, "0"):
            try:
                primary_id = int(primary_category_id)
            except Exception:
                primary_id = None

        if primary_id and primary_id not in category_ids:
            category_ids = [primary_id] + category_ids

        if category_ids and not primary_id:
            primary_id = category_ids[0]

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM article_categories WHERE article_id=%s", (article_id,))
            if category_ids:
                rows = []
                for cid in category_ids:
                    rows.append(
                        (article_id, int(cid), 1 if primary_id and int(
                            cid) == int(primary_id) else 0)
                    )
                cur.executemany(
                    "INSERT INTO article_categories (article_id, category_id, is_primary) VALUES (%s, %s, %s)",
                    rows,
                )
            conn.commit()
        except Exception as e:
            logging.exception("Error setting article categories: %s", e)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def set_article_tags(article_id, tag_ids):
        """
        Replaces article tags with the provided list of tag IDs.
        """
        article_id = int(article_id)
        tag_ids = ArticleService._coerce_int_list(tag_ids)

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM article_tags WHERE article_id=%s", (article_id,))
            if tag_ids:
                rows = [(article_id, int(tid)) for tid in tag_ids]
                cur.executemany(
                    "INSERT INTO article_tags (article_id, tag_id) VALUES (%s, %s)", rows)
            conn.commit()
        except Exception as e:
            logging.exception("Error setting article tags: %s", e)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def _get_article_categories(article_id):
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT c.*, ac.is_primary
                FROM article_categories ac
                JOIN categories c ON c.id = ac.category_id
                WHERE ac.article_id = %s
                ORDER BY ac.is_primary DESC, c.sort_order ASC, c.name ASC
                """,
                (int(article_id),),
            )
            return cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def _get_article_tags(article_id):
        """
        V1 fix: avoid alias `at` (can be problematic/unclear).
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT t.*
                FROM article_tags art
                JOIN tags t ON t.id = art.tag_id
                WHERE art.article_id = %s
                ORDER BY t.name ASC
                """,
                (int(article_id),),
            )
            return cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def _attach_taxonomy(article):
        if not article:
            return article
        aid = article.get("id")
        article["categories"] = ArticleService._get_article_categories(aid)
        article["tags"] = ArticleService._get_article_tags(aid)
        article["primary_category"] = None
        for c in article["categories"]:
            if c.get("is_primary"):
                article["primary_category"] = c
                break
        return article

    @staticmethod
    def attach_taxonomy_to_articles(items):
        """
        Convenience helper for list pages.
        Keeps behavior consistent with _attach_taxonomy but for a list.

        Note: This is N+1 (calls per article). Use only for small lists.
        """
        out = []
        for a in (items or []):
            try:
                out.append(ArticleService._attach_taxonomy(a))
            except Exception:
                out.append(a)
        return out

    # =============================================================================
    # Related articles
    # =============================================================================

    @staticmethod
    def get_related_articles(article_id, primary_category_id=None, limit=4):
        """
        Related articles for detail page:
        - Prefer same primary category (if provided)
        - Fallback to latest published
        - Excludes current article
        """
        article_id = int(article_id)
        limit = int(limit or 4)

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            if primary_category_id:
                try:
                    pcid = int(primary_category_id)
                except Exception:
                    pcid = None

                if pcid:
                    cur.execute(
                        """
                        SELECT DISTINCT a.*
                        FROM articles a
                        JOIN article_categories ac ON ac.article_id = a.id
                        WHERE ac.category_id = %s
                          AND a.id <> %s
                          AND a.status='published'
                          AND a.published_at IS NOT NULL
                          AND a.published
                          AND a.published_at <= NOW()
                        ORDER BY a.published_at DESC, a.updated_at DESC
                        LIMIT %s
                        """,
                        (pcid, article_id, limit),
                    )
                    rows = cur.fetchall() or []
                    if rows:
                        return rows

            cur.execute(
                """
                SELECT a.*
                FROM articles a
                WHERE a.id <> %s
                  AND a.status='published'
                  AND a.published_at IS NOT NULL
                  AND a.published_at <= NOW()
                ORDER BY a.published_at DESC, a.updated_at DESC
                LIMIT %s
                """,
                (article_id, limit),
            )
            return cur.fetchall() or []
        except Exception as e:
            logging.exception("Error fetching related articles: %s", e)
            return []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # =============================================================================
    # Articles CRUD + queries
    # =============================================================================

    @staticmethod
    def get_article_by_id(article_id, include_taxonomy=True):
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM articles WHERE id=%s",
                        (int(article_id),))
            row = cur.fetchone()
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

        return ArticleService._attach_taxonomy(row) if include_taxonomy else row

    @staticmethod
    def get_public_article_by_slug(slug, include_taxonomy=True):
        slug = (slug or "").strip()
        if not slug:
            return None

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT * FROM articles
                WHERE slug=%s
                  AND status='published'
                  AND published_at IS NOT NULL
                  AND published_at <= NOW()
                LIMIT 1
                """,
                (slug,),
            )
            row = cur.fetchone()
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

        return ArticleService._attach_taxonomy(row) if include_taxonomy else row

    @staticmethod
    def get_all_articles(include_taxonomy=False):
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT * FROM articles
                ORDER BY
                  CASE WHEN status='published' THEN 0 ELSE 1 END,
                  published_at DESC,
                  updated_at DESC
                """
            )
            rows = cur.fetchall() or []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

        if not include_taxonomy:
            return rows
        return [ArticleService._attach_taxonomy(r) for r in rows]

    @staticmethod
    def get_public_articles(
        page=1,
        page_size=10,
        category_slug=None,
        tag_slug=None,
        keyword=None,
        q=None,  # alias for public ?q=
        attach_taxonomy=False,
    ):
        """
        Public list:
          - /news/ (no filters)
          - /news/{category_slug} (category filter)
          - /news/tag/{tag_slug} (tag filter)
          - /news/?q=... (keyword search)

        Notes:
        - Uses DISTINCT + COUNT(DISTINCT) to avoid duplicates from joins.
        - Tag join uses alias `art` (not `at`).
        - attach_taxonomy defaults False for speed (N+1 if True).
        """
        page = ArticleService._normalize_page(page, default=1)
        page_size = ArticleService._normalize_page_size(
            page_size, default=10, max_size=100)
        offset = (page - 1) * page_size

        if keyword is None and q is not None:
            keyword = q
        keyword = (str(keyword).strip() if keyword is not None else "") or None

        category_slug = (str(category_slug).strip()
                         if category_slug is not None else "") or None
        tag_slug = (str(tag_slug).strip()
                    if tag_slug is not None else "") or None

        where = [
            "a.status='published'",
            "a.published_at IS NOT NULL",
            "a.published_at <= NOW()",
        ]
        params = []
        join_sql = ""

        if category_slug:
            join_sql += (
                " JOIN article_categories ac ON ac.article_id = a.id "
                " JOIN categories c ON c.id = ac.category_id "
            )
            where.append("c.slug = %s")
            params.append(category_slug)

        if tag_slug:
            join_sql += (
                " JOIN article_tags art ON art.article_id = a.id "
                " JOIN tags t ON t.id = art.tag_id "
            )
            where.append("t.slug = %s")
            params.append(tag_slug)

        if keyword:
            kw = f"%{keyword}%"
            where.append(
                "(a.title LIKE %s OR a.summary LIKE %s OR a.content LIKE %s)")
            params.extend([kw, kw, kw])

        where_sql = " AND ".join(where)

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                f"""
                SELECT DISTINCT a.*
                FROM articles a
                {join_sql}
                WHERE {where_sql}
                ORDER BY a.published_at DESC, a.updated_at DESC
                LIMIT %s OFFSET %s
                """,
                (*params, page_size, offset),
            )
            items = cur.fetchall() or []

            cur.execute(
                f"""
                SELECT COUNT(DISTINCT a.id) AS cnt
                FROM articles a
                {join_sql}
                WHERE {where_sql}
                """,
                tuple(params),
            )
            total = int((cur.fetchone() or {}).get("cnt") or 0)

            if attach_taxonomy and items:
                items = ArticleService.attach_taxonomy_to_articles(items)

            return {"items": items, "total": total, "page": page, "page_size": page_size}
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def create_article(data):
        """
        Expected keys:
          title, slug?, summary?, content, status,
          cover_image_path?, cover_image_alt?,
          category_ids (list/int/str), primary_category_id (int/str),
          tag_names (list[str]) OR tag_ids (list[int])
        """
        title = (data.get("title") or "").strip()
        content = data.get("content")

        if not title or not content:
            return None

        status = ArticleService._coerce_status(data.get("status"))

        slug = (data.get("slug") or "").strip()
        if not slug:
            slug = ArticleService._slugify(title)
        slug = ArticleService._ensure_unique_article_slug(slug)

        summary = data.get("summary")
        if summary == "":
            summary = None

        cover_image_path = data.get("cover_image_path")
        if cover_image_path == "":
            cover_image_path = None

        cover_image_alt = data.get("cover_image_alt")
        if cover_image_alt == "":
            cover_image_alt = None

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            # published_at uses DB time (NOW()) when status is published
            cur.execute(
                """
                INSERT INTO articles (title, slug, summary, content, status, published_at, cover_image_path, cover_image_alt)
                VALUES (%s, %s, %s, %s, %s,
                        CASE WHEN %s='published' THEN NOW() ELSE NULL END,
                        %s, %s)
                """,
                (title, slug, summary, content, status,
                 status, cover_image_path, cover_image_alt),
            )
            conn.commit()
            article_id = cur.lastrowid
        except Exception as e:
            logging.exception("Error inserting article: %s", e)
            return None
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

        # Categories
        ArticleService.set_article_categories(
            article_id=article_id,
            category_ids=data.get("category_ids"),
            primary_category_id=data.get("primary_category_id"),
        )

        # Tags
        if data.get("tag_names"):
            tags = ArticleService.get_or_create_tags_by_names(
                data.get("tag_names"))
            tag_ids = [t["id"] for t in tags if t and t.get("id")]
            ArticleService.set_article_tags(article_id, tag_ids=tag_ids)
        else:
            ArticleService.set_article_tags(
                article_id,
                tag_ids=ArticleService._coerce_int_list(data.get("tag_ids")),
            )

        return ArticleService.get_article_by_id(article_id, include_taxonomy=True)

    @staticmethod
    def update_article(article_id, data):
        """
        Update article + taxonomy.

        Notes:
        - updated_at is DB-managed (ON UPDATE CURRENT_TIMESTAMP); do not set it here.
        - published_at: if switching draft -> published and published_at is NULL, set NOW().
        - if switching published -> draft, we keep published_at as-is (history).
        """
        article_id = int(article_id)
        existing = ArticleService.get_article_by_id(
            article_id, include_taxonomy=False)
        if not existing:
            return None

        title = (data.get("title") or existing.get("title") or "").strip()

        content = data.get("content")
        if content in (None, ""):
            content = existing.get("content")

        status = ArticleService._coerce_status(
            data.get("status") or existing.get("status"))

        slug = (data.get("slug") or existing.get("slug") or "").strip()
        if not slug:
            slug = ArticleService._slugify(title)
        slug = ArticleService._ensure_unique_article_slug(
            slug, exclude_id=article_id)

        summary = data.get("summary")
        if summary == "":
            summary = None
        if summary is None and "summary" not in data:
            summary = existing.get("summary")

        cover_image_path = data.get("cover_image_path")
        if cover_image_path == "":
            cover_image_path = None
        if cover_image_path is None and "cover_image_path" not in data:
            cover_image_path = existing.get("cover_image_path")

        cover_image_alt = data.get("cover_image_alt")
        if cover_image_alt == "":
            cover_image_alt = None
        if cover_image_alt is None and "cover_image_alt" not in data:
            cover_image_alt = existing.get("cover_image_alt")

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                UPDATE articles
                SET title=%s,
                    slug=%s,
                    summary=%s,
                    content=%s,
                    status=%s,
                    published_at=CASE
                        WHEN %s='published' AND published_at IS NULL THEN NOW()
                        ELSE published_at
                    END,
                    cover_image_path=%s,
                    cover_image_alt=%s
                WHERE id=%s
                """,
                (title, slug, summary, content, status, status,
                 cover_image_path, cover_image_alt, article_id),
            )
            conn.commit()
        except Exception as e:
            logging.exception("Error updating article: %s", e)
            return None
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

        # Categories
        if "category_ids" in data or "primary_category_id" in data:
            ArticleService.set_article_categories(
                article_id=article_id,
                category_ids=data.get("category_ids"),
                primary_category_id=data.get("primary_category_id"),
            )

        # Tags
        if "tag_names" in data:
            tags = ArticleService.get_or_create_tags_by_names(
                data.get("tag_names"))
            tag_ids = [t["id"] for t in tags if t and t.get("id")]
            ArticleService.set_article_tags(article_id, tag_ids=tag_ids)
        elif "tag_ids" in data:
            ArticleService.set_article_tags(
                article_id=article_id,
                tag_ids=ArticleService._coerce_int_list(data.get("tag_ids")),
            )

        return ArticleService.get_article_by_id(article_id, include_taxonomy=True)

    @staticmethod
    def delete_article(article_id):
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM articles WHERE id=%s", (int(article_id),))
            conn.commit()
        except Exception as e:
            logging.exception("Error deleting article: %s", e)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def increment_article_view(article_id):
        """
        Atomic increment (counts all hits, including bots).
        """
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE articles SET view_count = view_count + 1 WHERE id=%s",
                (int(article_id),),
            )
            conn.commit()
        except Exception as e:
            logging.exception("Error incrementing view_count: %s", e)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def search_articles(keyword=None, status=None, page=1, page_size=20):
        """
        Admin search (no joins by default).
        """
        where = []
        params = []

        keyword = (str(keyword).strip() if keyword is not None else "") or None
        status = (str(status).strip().lower()
                  if status is not None else "") or None

        if keyword:
            where.append(
                "(title LIKE %s OR summary LIKE %s OR content LIKE %s)")
            kw = f"%{keyword}%"
            params.extend([kw, kw, kw])

        if status:
            where.append("status = %s")
            params.append(status)

        where_sql = " AND ".join(where) if where else "1=1"
        page = ArticleService._normalize_page(page, default=1)
        page_size = ArticleService._normalize_page_size(
            page_size, default=20, max_size=200)
        offset = (page - 1) * page_size

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                f"""
                SELECT * FROM articles
                WHERE {where_sql}
                ORDER BY published_at DESC, updated_at DESC
                LIMIT %s OFFSET %s
                """,
                (*params, page_size, offset),
            )
            items = cur.fetchall() or []

            cur.execute(
                f"SELECT COUNT(*) as cnt FROM articles WHERE {where_sql}",
                tuple(params),
            )
            total = int((cur.fetchone() or {}).get("cnt") or 0)

            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
            }
        except Exception as e:
            logging.exception("Error searching articles: %s", e)
            return {
                "items": [],
                "total": 0,
                "page": ArticleService._normalize_page(page, default=1),
                "page_size": ArticleService._normalize_page_size(page_size, default=20, max_size=200),
            }
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
