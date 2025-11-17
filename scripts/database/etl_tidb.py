import argparse
import json
import logging
import time
from typing import Dict, List, Tuple

from db import db_cursor, insert_many
from fetch_courses import fetch_all, normalize_for_db
from fetch_professors import fetch_professors_rows


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _insert_etl_run_start(semester_id: int) -> int:
    # ensure table exists
    try:
        with db_cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS etl_runs (
                    run_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    semester_id INT,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    finished_at TIMESTAMP NULL,
                    status ENUM('success','failure') NOT NULL,
                    error TEXT,
                    counts JSON,
                    INDEX idx_semester_id (semester_id),
                    INDEX idx_status (status),
                    INDEX idx_started_at (started_at)
                ) COMMENT='ETL execution audit and metrics'
                """
            )
    except Exception:
        logger.warning("etl_runs ensure failed; will attempt insert anyway")

    try:
        with db_cursor() as cur:
            cur.execute(
                "INSERT INTO etl_runs (semester_id, status, counts) VALUES (%s, %s, %s)",
                (semester_id, "failure", json.dumps({})),
            )
            return cur.lastrowid
    except Exception as e:
        logger.warning("etl_runs insert failed; continuing without run tracking: %s", e)
        return 0


def _update_etl_run_finish(run_id: int, *, status: str, counts: Dict[str, int], error: str = None):
    if not run_id:
        return
    with db_cursor() as cur:
        cur.execute(
            "UPDATE etl_runs SET finished_at=CURRENT_TIMESTAMP, status=%s, counts=%s, error=%s WHERE run_id=%s",
            (status, json.dumps(counts), error, run_id),
        )


def _parse_semester_meta(semester_id: int):
    sid = str(semester_id)
    year = int(sid[:-1]) if len(sid) >= 2 else semester_id
    code = sid[-1] if sid else ""
    term_map = {"1": "Spring", "3": "Fall", "2": "Summer"}
    term = term_map.get(code, f"Term {code}")
    name = f"{term} {year}"
    return year, term, name


def _ensure_semester(semester_id: int):
    year, term, name = _parse_semester_meta(semester_id)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO semesters (semester_id, semester_name, year, term, is_active)
            VALUES (%s, %s, %s, %s, FALSE)
            ON DUPLICATE KEY UPDATE
                semester_name=VALUES(semester_name),
                year=VALUES(year),
                term=VALUES(term)
            """,
            (semester_id, name, year, term),
        )


def load_to_staging(semester_id: int, rows: Dict[str, List[Tuple]]):
    counts: Dict[str, int] = {}
    with db_cursor() as cur:
        # global entities (upsert)
        counts["staging_schools"] = insert_many(
            cur,
            "staging_schools",
            ["school_id", "school_name"],
            rows.get("schools", []),
            on_duplicate_update_cols=["school_name"],
        )
        counts["staging_programs"] = insert_many(
            cur,
            "staging_programs",
            ["school_id", "program_id", "program_name"],
            rows.get("programs", []),
            on_duplicate_update_cols=["program_name"],
        )
        # seed professors (from USC instructor names)
        # Deduplicate professor seeds and drop empty names to avoid PK collisions
        seed = rows.get("professors_seed", [])
        seen_names = set()
        dedup_seed = []
        for tup in seed:
            name = (tup[0] or "").strip()
            if not name or name in seen_names:
                continue
            seen_names.add(name)
            dedup_seed.append(tup)

        counts["staging_professors_seed"] = insert_many(
            cur,
            "staging_professors",
            [
                "professor_name",
                "rmp_id",
                "difficulty",
                "rating",
                "rating_count",
                "take_again_percentage",
            ],
            dedup_seed,
            on_duplicate_update_cols=["professor_name"],
        )

        # Clean semester-scoped staging to avoid PK collisions on reruns
        for table in [
            "staging_section_duplicated_credits",
            "staging_section_prerequisites",
            "staging_section_instructors",
            "staging_course_ge_categories",
            "staging_sections",
            "staging_courses",
        ]:
            cur.execute(f"DELETE FROM {table} WHERE semester_id=%s", (semester_id,))

        # semester-scoped entities
        counts["staging_courses"] = insert_many(
            cur,
            "staging_courses",
            [
                "semester_id",
                "course_id",
                "program_id",
                "course_number",
                "title",
                "description",
            ],
            rows.get("courses", []),
            on_duplicate_update_cols=["program_id", "course_number", "title", "description"],
        )
        counts["staging_sections"] = insert_many(
            cur,
            "staging_sections",
            [
                "semester_id",
                "section_id",
                "course_id",
                "section_type",
                "units",
                "total_seats",
                "registered_seats",
                "location",
                "time_schedule",
                "d_clearance_required",
            ],
            rows.get("sections", []),
            on_duplicate_update_cols=[
                "section_type",
                "units",
                "total_seats",
                "registered_seats",
                "location",
                "time_schedule",
                "d_clearance_required",
            ],
        )
        counts["staging_section_instructors"] = insert_many(
            cur,
            "staging_section_instructors",
            ["semester_id", "section_id", "professor_name"],
            rows.get("section_instructors", []),
        )
        counts["staging_course_ge_categories"] = insert_many(
            cur,
            "staging_course_ge_categories",
            ["semester_id", "course_id", "ge_category"],
            rows.get("course_ge_categories", []),
        )
        counts["staging_section_prerequisites"] = insert_many(
            cur,
            "staging_section_prerequisites",
            ["semester_id", "section_id", "prerequisite_text"],
            rows.get("section_prerequisites", []),
        )
        counts["staging_section_duplicated_credits"] = insert_many(
            cur,
            "staging_section_duplicated_credits",
            ["semester_id", "section_id", "duplicated_text"],
            rows.get("section_duplicated_credits", []),
        )
    return counts


def upsert_professors(rows: List[Tuple[str, int, float, float, int, float]]):
    if not rows:
        return 0
    with db_cursor() as cur:
        return insert_many(
            cur,
            "staging_professors",
            [
                "professor_name",
                "rmp_id",
                "difficulty",
                "rating",
                "rating_count",
                "take_again_percentage",
            ],
            rows,
            on_duplicate_update_cols=[
                "rmp_id",
                "difficulty",
                "rating",
                "rating_count",
                "take_again_percentage",
            ],
        )


def validate_staging(semester_id: int):
    with db_cursor() as cur:
        # orphan sections
        cur.execute(
            """
            SELECT COUNT(*) FROM staging_sections s
            LEFT JOIN staging_courses c
            ON c.semester_id=s.semester_id AND c.course_id=s.course_id
            WHERE s.semester_id=%s AND c.course_id IS NULL
            """,
            (semester_id,),
        )
        (orphan_sections,) = cur.fetchone()
        if orphan_sections:
            raise RuntimeError(f"Validation failed: {orphan_sections} sections missing parent course")


def promote_semester(semester_id: int):
    # Ensure reference data exists
    _ensure_semester(semester_id)
    with db_cursor() as cur:
        # Upsert global tables from staging (schools, programs, professors)
        cur.execute(
            """
            INSERT INTO schools (school_id, school_name)
            SELECT school_id, school_name FROM staging_schools
            ON DUPLICATE KEY UPDATE school_name=VALUES(school_name)
            """
        )
        cur.execute(
            """
            INSERT INTO programs (school_id, program_id, program_name)
            SELECT school_id, program_id, program_name FROM staging_programs
            ON DUPLICATE KEY UPDATE program_name=VALUES(program_name)
            """
        )
        cur.execute(
            """
            INSERT INTO professors (
                professor_name, rmp_id, difficulty, rating, rating_count, take_again_percentage
            )
            SELECT professor_name, rmp_id, difficulty, rating, rating_count, take_again_percentage
            FROM staging_professors
            ON DUPLICATE KEY UPDATE
                rmp_id=VALUES(rmp_id),
                difficulty=VALUES(difficulty),
                rating=VALUES(rating),
                rating_count=VALUES(rating_count),
                take_again_percentage=VALUES(take_again_percentage)
            """
        )

        # delete in dependency order
        for table in [
            "section_duplicated_credits",
            "section_prerequisites",
            "section_instructors",
            "course_ge_categories",
            "sections",
            "courses",
        ]:
            cur.execute(f"DELETE FROM {table} WHERE semester_id=%s", (semester_id,))

        # insert courses
        cur.execute(
            """
            INSERT INTO courses (semester_id, course_id, program_id, course_number, title, description)
            SELECT semester_id, course_id, program_id, course_number, title, description
            FROM staging_courses WHERE semester_id=%s
            """,
            (semester_id,),
        )
        # sections
        cur.execute(
            """
            INSERT INTO sections (
                semester_id, section_id, course_id, section_type, units, total_seats,
                registered_seats, location, time_schedule, d_clearance_required
            )
            SELECT semester_id, section_id, course_id, section_type, units, total_seats,
                   registered_seats, location, time_schedule, d_clearance_required
            FROM staging_sections WHERE semester_id=%s
            """,
            (semester_id,),
        )
        # instructors
        cur.execute(
            """
            INSERT INTO section_instructors (semester_id, section_id, professor_name)
            SELECT semester_id, section_id, professor_name
            FROM staging_section_instructors WHERE semester_id=%s
            """,
            (semester_id,),
        )
        # prerequisites
        cur.execute(
            """
            INSERT INTO section_prerequisites (semester_id, section_id, prerequisite_text)
            SELECT semester_id, section_id, prerequisite_text
            FROM staging_section_prerequisites WHERE semester_id=%s
            """,
            (semester_id,),
        )
        # duplicated credits
        cur.execute(
            """
            INSERT INTO section_duplicated_credits (semester_id, section_id, duplicated_text)
            SELECT semester_id, section_id, duplicated_text
            FROM staging_section_duplicated_credits WHERE semester_id=%s
            """,
            (semester_id,),
        )
        # ge categories
        cur.execute(
            """
            INSERT INTO course_ge_categories (semester_id, course_id, ge_category)
            SELECT semester_id, course_id, ge_category
            FROM staging_course_ge_categories WHERE semester_id=%s
            """,
            (semester_id,),
        )


def main():
    parser = argparse.ArgumentParser(description="CourseX TiDB ETL")
    parser.add_argument("--semester-id", required=True, help="e.g., 20261")
    parser.add_argument("--concurrency", type=int, default=12)
    parser.add_argument("--update-professors", choices=["yes", "no"], default="yes")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    semester_id = int(args.semester_id)
    run_id = _insert_etl_run_start(semester_id)
    counts: Dict[str, int] = {}

    try:
        logger.info("Fetching USC data for %s", semester_id)
        schools, courses_by_school, ge_payloads = fetch_all(args.semester_id, concurrency=args.concurrency)
        logger.info("Normalizing data")
        rows = normalize_for_db(args.semester_id, schools, courses_by_school, ge_payloads)

        if args.update_professors == "yes":
            logger.info("Fetching RMP professors")
            try:
                prof_rows = fetch_professors_rows()
            except Exception as e:
                logger.warning("RMP fetch failed; continuing without update: %s", e)
                prof_rows = []
        else:
            prof_rows = []

        logger.info("Loading into staging")
        # Upsert professors from RMP first to have metrics over seeded names
        if prof_rows:
            inserted = upsert_professors(prof_rows)
            counts["staging_professors_rmp"] = inserted
        batch_counts = load_to_staging(semester_id, rows)
        counts.update(batch_counts)

        logger.info("Validating staging data")
        validate_staging(semester_id)

        if args.dry_run:
            logger.info("Dry run complete; skipping promotion")
            _update_etl_run_finish(run_id, status="success", counts=counts)
            return

        logger.info("Promoting semester %s to production", semester_id)
        promote_semester(semester_id)

        # Optional: clean semester-scoped staging tables
        with db_cursor() as cur:
            for table in [
                "staging_section_duplicated_credits",
                "staging_section_prerequisites",
                "staging_section_instructors",
                "staging_course_ge_categories",
                "staging_sections",
                "staging_courses",
            ]:
                cur.execute(f"DELETE FROM {table} WHERE semester_id=%s", (semester_id,))

        _update_etl_run_finish(run_id, status="success", counts=counts)
        logger.info("ETL finished successfully")
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        _update_etl_run_finish(run_id, status="failure", counts=counts, error=str(e))
        raise


if __name__ == "__main__":
    main()


