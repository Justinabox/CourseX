import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import hashlib

import requests


logger = logging.getLogger(__name__)


# -------------------------
# Normalization helpers
# -------------------------

def _safe_course_code(course: Dict[str, Any], preferred_prefix: Optional[str] = None) -> Optional[str]:
    try:
        scheduled = course.get("scheduledCourseCode") or {}
        matched = course.get("matchedCourseCode") or {}
        published = course.get("publishedCourseCode") or {}

        candidates = [scheduled, matched, published]
        if preferred_prefix:
            for c in candidates:
                if (c or {}).get("prefix") == preferred_prefix and c.get("courseHyphen"):
                    return c.get("courseHyphen")
        for c in candidates:
            if c.get("courseHyphen"):
                return c.get("courseHyphen")
    except Exception:
        pass
    return None


def _parse_units(units_value: Any) -> Optional[str]:
    if units_value is None:
        return None
    try:
        value = units_value
        if isinstance(value, list):
            value = value[0] if value else None
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return str(int(value)) if float(value).is_integer() else str(float(value))
        if isinstance(value, str):
            text = value.strip()
            if "-" in text or "–" in text:
                return text
            try:
                num = float(text)
                return str(int(num)) if num.is_integer() else str(num)
            except Exception:
                return text
        return str(value)
    except Exception:
        return str(units_value)


_DAY_NAME_TO_ABBR = {
    "Mon": "M",
    "Tue": "T",
    "Wed": "W",
    "Thu": "Th",
    "Fri": "F",
    "Sat": "Sa",
    "Sun": "Su",
}


def _format_days(days_list: Sequence[str], fallback_day_code: Optional[str]) -> Optional[str]:
    try:
        days_list = days_list or []
        if days_list:
            abbrs = [_DAY_NAME_TO_ABBR.get(d, (d or "")[:2]) for d in days_list if d]
            return "".join(abbrs) if abbrs else None
        code = (fallback_day_code or "").strip().upper()
        if not code:
            return None
        return code.replace("H", "Th")
    except Exception:
        return None


def _format_time(schedule_entries: Sequence[Dict[str, Any]]) -> str:
    try:
        schedule_entries = schedule_entries or []
        if not schedule_entries:
            return "TBA"
        formatted: List[str] = []
        for entry in schedule_entries:
            days = entry.get("days") or []
            day_code = entry.get("dayCode")
            start = entry.get("startTime") or ""
            end = entry.get("endTime") or ""
            day_str = _format_days(days, day_code) or ""
            if not (day_str or start or end):
                continue
            if start and end:
                formatted.append(f"{day_str} {start} - {end}".strip())
            elif start:
                formatted.append(f"{day_str} {start}".strip())
            else:
                formatted.append(day_str)
        if not formatted:
            return "TBA"
        unique: List[str] = []
        seen = set()
        for f in formatted:
            if f not in seen:
                unique.append(f)
                seen.add(f)
        return unique[0] if len(unique) == 1 else f"{unique[0]} (+{len(unique) - 1} more)"
    except Exception:
        return "TBA"


def _split_duplicate_credit(text: Any) -> List[str]:
    if not isinstance(text, str):
        return []
    parts: List[str] = []
    for chunk in text.replace("/", ",").replace(";", ",").split(","):
        for sub in chunk.split(" and "):
            value = sub.strip()
            if value:
                parts.append(value)
    return parts


def _map_section_type(value: Optional[str]) -> str:
    v = (value or "").lower()
    if "lecture" in v:
        return "Lecture"
    if "discussion" in v:
        return "Discussion"
    if "lab" in v:
        return "Lab"
    if "quiz" in v:
        return "Quiz"
    if "studio" in v:
        return "Studio"
    return "Other"


# -------------------------
# Fetchers
# -------------------------

def _request_json(url: str, *, timeout: int = 60) -> Dict[str, Any]:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_schools(term_code: str) -> List[Dict[str, Any]]:
    data = _request_json(
        f"https://classes.usc.edu/api/Schools/TermCode?termCode={term_code}"
    )
    # Append GE school with default GESM program
    schools: List[Dict[str, Any]] = [
        {
            "name": "General Education",
            "prefix": "GE",
            "programs": [{"name": "GE Seminar", "prefix": "GESM"}],
        }
    ]
    for school in data:
        programs = [
            {"name": p.get("name"), "prefix": p.get("prefix")} for p in school.get("programs", [])
        ]
        schools.append(
            {
                "name": school.get("name"),
                "prefix": school.get("prefix"),
                "programs": programs,
            }
        )
    return schools


def _process_course(course: Dict[str, Any], preferred_prefix: Optional[str]) -> List[Dict[str, Any]]:
    sections_output: List[Dict[str, Any]] = []
    for section in course.get("sections") or []:
        if section.get("isCancelled"):
            continue
        schedule_entries = section.get("schedule") or []
        first_schedule = schedule_entries[0] if schedule_entries else {}

        duplicate_credit_value = course.get("duplicateCredit") or ""
        duplicated_credits_list = _split_duplicate_credit(duplicate_credit_value)

        prerequisite_codes = course.get("prerequisiteCourseCodes") or []
        prerequisites_list: List[str] = []
        for prerequisite in prerequisite_codes:
            try:
                options = prerequisite.get("courseOptions") or []
                if not options:
                    continue
                code = (options[0] or {}).get("courseHyphen")
                if code:
                    prerequisites_list.append(code)
            except Exception:
                continue

        instructors: List[str] = []
        for instructor in section.get("instructors") or []:
            first_name = (instructor or {}).get("firstName") or ""
            last_name = (instructor or {}).get("lastName") or ""
            full_name = (first_name + " " + last_name).strip()
            if full_name:
                instructors.append(full_name)

        course_code = _safe_course_code(course, preferred_prefix)
        title_value = (
            section.get("name")
            or course.get("name")
            or course.get("fullCourseName")
            or ((course.get("publishedCourseCode") or {}).get("courseSpace"))
        )
        description_value = course.get("description")
        units_value = _parse_units(section.get("units"))
        time_string = _format_time(schedule_entries)
        rnr_mode = section.get("rnrMode")

        sections_output.append(
            {
                "title": title_value,
                "description": description_value,
                "courseCode": course_code,
                "section": {
                    "sectionCode": section.get("sisSectionId"),
                    "instructors": instructors,
                    "units": units_value,
                    "total": section.get("totalSeats"),
                    "registered": section.get("registeredSeats"),
                    "location": first_schedule.get("location"),
                    "time": time_string,
                    "duplicatedCredits": duplicated_credits_list,
                    "prerequisites": prerequisites_list,
                    "dClearance": section.get("hasDClearance"),
                    "type": _map_section_type(rnr_mode),
                },
            }
        )
    return sections_output


def fetch_program_courses(term_code: str, school_code: str, program_code: str) -> List[Dict[str, Any]]:
    last_error: Optional[Exception] = None
    for attempt_index in range(1, 5):
        try:
            data = _request_json(
                f"https://classes.usc.edu/api/Courses/CoursesByTermSchoolProgram?termCode={term_code}&school={school_code}&program={program_code}"
            )
            aggregation: Dict[Tuple[Optional[str], Optional[str], Optional[str]], Dict[str, Any]] = {}
            for course in data.get("courses", []):
                processed_sections = _process_course(course, preferred_prefix=program_code)
                for item in processed_sections:
                    key = (item.get("title"), item.get("description"), item.get("courseCode"))
                    if key not in aggregation:
                        aggregation[key] = {
                            "title": item.get("title"),
                            "description": item.get("description"),
                            "courseCode": item.get("courseCode"),
                            "sections": [],
                            "_seen": set(),
                        }
                    section_obj = item.get("section") or {}
                    sec_code = section_obj.get("sectionCode")
                    seen = aggregation[key]["_seen"]
                    if sec_code and sec_code in seen:
                        continue
                    if sec_code:
                        seen.add(sec_code)
                    aggregation[key]["sections"].append(section_obj)
            result: List[Dict[str, Any]] = []
            for v in aggregation.values():
                v.pop("_seen", None)
                result.append(v)
            return result
        except Exception as error:
            last_error = error
            if attempt_index < 4:
                wait_seconds = attempt_index * 5
                logger.warning(
                    "Attempt %s failed for %s/%s: %s. Retrying in %ss…",
                    attempt_index,
                    school_code,
                    program_code,
                    error,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
            else:
                break
    if last_error:
        raise last_error
    return []


def fetch_ge(term_code: str, ge_type: str, category_prefix: str) -> Dict[str, Any]:
    last_error: Optional[Exception] = None
    for attempt_index in range(1, 5):
        try:
            return _request_json(
                f"https://classes.usc.edu/api/Courses/GeCoursesByTerm?termCode={term_code}&geRequirementPrefix={ge_type}&categoryPrefix={category_prefix}"
            )
        except Exception as error:
            last_error = error
            if attempt_index < 4:
                wait_seconds = attempt_index * 5
                logger.warning(
                    "Attempt %s failed for GE %s/%s: %s. Retrying in %ss…",
                    attempt_index,
                    ge_type,
                    category_prefix,
                    error,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
            else:
                break
    if last_error:
        raise last_error
    return {}


# -------------------------
# Normalized entity mapping
# -------------------------

GE_CATEGORY_MAP: List[Tuple[str, str, str]] = [
    ("ACORELIT", "ARTS", "A"),
    ("ACORELIT", "HINQ", "B"),
    ("ACORELIT", "SANA", "C"),
    ("ACORELIT", "LIFE", "D"),
    ("ACORELIT", "PSC", "E"),
    ("ACORELIT", "QREA", "F"),
    ("AGLOPERS", "GPG", "G"),
    ("AGLOPERS", "GPH", "H"),
]


def _course_number_from_code(course_code: str) -> Optional[int]:
    try:
        # Expect PREFIX-NUM like CS-103
        parts = course_code.split("-")
        if len(parts) < 2:
            return None
        num = "".join(ch for ch in parts[1] if ch.isdigit())
        return int(num) if num else None
    except Exception:
        return None


def build_program_to_school(schools: List[Dict[str, Any]]) -> Dict[str, str]:
    index: Dict[str, str] = {}
    for school in schools:
        s_prefix = school.get("prefix")
        for program in school.get("programs", []):
            p_prefix = program.get("prefix")
            if p_prefix and s_prefix:
                index[p_prefix] = s_prefix
    return index


def normalize_for_db(
    term_code: str,
    schools: List[Dict[str, Any]],
    courses_by_school: Dict[str, Dict[str, List[Dict[str, Any]]]],
    ge_courses_payloads: List[Tuple[str, str, Dict[str, Any]]],
) -> Dict[str, List[Tuple]]:
    semester_id = int(term_code)

    # schools/programs
    school_rows: List[Tuple[str, str]] = []
    program_rows: List[Tuple[str, str, str]] = []
    for s in schools:
        school_id = s.get("prefix")
        school_name = s.get("name")
        if school_id and school_name:
            school_rows.append((school_id, school_name))
        for p in s.get("programs", []):
            program_id = p.get("prefix")
            program_name = p.get("name")
            if program_id and program_name and school_id:
                program_rows.append((school_id, program_id, program_name))

    # core course data
    course_rows: List[Tuple[int, str, str, Optional[int], str, Optional[str]]] = []
    section_rows: List[Tuple[int, int, str, str, str, int, int, Optional[str], Optional[str], bool]] = []
    instructor_rows: List[Tuple[int, int, str]] = []
    ge_rows: List[Tuple[int, str, str]] = []
    prereq_rows: List[Tuple[int, int, str]] = []
    dup_rows: List[Tuple[int, int, str]] = []
    professor_seed_rows: List[Tuple[str, Optional[int], Optional[float], Optional[float], Optional[int], Optional[float]]] = []

    # global de-dup sets across the term to avoid unique key collisions
    seen_instructor_keys: set = set()  # (semester_id, section_id, name_lower)
    seen_prereq_keys: set = set()      # (semester_id, section_id, text_lower)
    seen_dupcredit_keys: set = set()   # (semester_id, section_id, text_lower)

    for school_id, programs in (courses_by_school or {}).items():
        for program_id, grouped_courses in (programs or {}).items():
            # detect duplicates by courseCode within this program
            code_counts: Dict[str, int] = {}
            for item in grouped_courses or []:
                c = item.get("courseCode")
                if not c:
                    continue
                code_counts[c] = code_counts.get(c, 0) + 1

            for item in grouped_courses or []:
                course_code = item.get("courseCode")
                title = item.get("title")
                description = item.get("description")
                if not course_code or not title:
                    continue
                course_number = _course_number_from_code(course_code)
                # custom course id suffix when same code appears with different title/desc
                final_course_id = course_code
                if code_counts.get(course_code, 0) > 1:
                    h = hashlib.sha1((title or "").strip().lower().encode("utf-8")).hexdigest()[:6]
                    final_course_id = f"{course_code}-{h}"
                course_rows.append((semester_id, final_course_id, program_id, course_number, title, description))

                # GE tags for this course
                tags = list({t for t in (item.get("GE") or [])})
                for t in tags:
                    ge_rows.append((semester_id, final_course_id, t))

                for section in item.get("sections", []) or []:
                    sec_id_raw = section.get("sectionCode")
                    try:
                        section_id = int(str(sec_id_raw))
                    except Exception:
                        # skip sections that do not have numeric sisSectionId
                        continue
                    # seed professors with null metrics to satisfy FK
                    for name in section.get("instructors", []) or []:
                        # normalize whitespace and case-insensitive key for uniqueness
                        norm_name = " ".join((name or "").split()).strip()
                        if not norm_name:
                            continue
                        ikey = (semester_id, section_id, norm_name.lower())
                        if ikey in seen_instructor_keys:
                            continue
                        seen_instructor_keys.add(ikey)
                        professor_seed_rows.append((norm_name, None, None, None, None, None))
                        instructor_rows.append((semester_id, section_id, norm_name))

                    units = section.get("units")
                    total = section.get("total") or 0
                    registered = section.get("registered") or 0
                    location = section.get("location")
                    time_str = section.get("time")
                    d_clearance = bool(section.get("dClearance"))
                    sec_type = section.get("type") or "Other"

                    section_rows.append(
                        (
                            semester_id,
                            section_id,
                            final_course_id,
                            sec_type,
                            units or "",
                            int(total),
                            int(registered),
                            location,
                            time_str,
                            d_clearance,
                        )
                    )

                    for dup in section.get("duplicatedCredits", []) or []:
                        norm_dup = (dup or "").strip()
                        if not norm_dup:
                            continue
                        dkey = (semester_id, section_id, norm_dup.lower())
                        if dkey in seen_dupcredit_keys:
                            continue
                        seen_dupcredit_keys.add(dkey)
                        dup_rows.append((semester_id, section_id, norm_dup))
                    for pre in section.get("prerequisites", []) or []:
                        norm_pre = (pre or "").strip()
                        if not norm_pre:
                            continue
                        pkey = (semester_id, section_id, norm_pre.lower())
                        if pkey in seen_prereq_keys:
                            continue
                        seen_prereq_keys.add(pkey)
                        prereq_rows.append((semester_id, section_id, norm_pre))

    return {
        "schools": school_rows,
        "programs": program_rows,
        "courses": course_rows,
        "sections": section_rows,
        "section_instructors": instructor_rows,
        "course_ge_categories": ge_rows,
        "section_prerequisites": prereq_rows,
        "section_duplicated_credits": dup_rows,
        "professors_seed": professor_seed_rows,
    }


def fetch_all(term_code: str, *, concurrency: int = 12) -> Tuple[
    List[Dict[str, Any]],
    Dict[str, Dict[str, List[Dict[str, Any]]]],
    List[Tuple[str, str, Dict[str, Any]]],
]:
    schools = fetch_schools(term_code)

    courses_by_school: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    tasks = []
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        for school in schools:
            school_prefix = school.get("prefix")
            if not school_prefix or school_prefix == "GE":
                continue
            for program in school.get("programs", []) or []:
                program_prefix = program.get("prefix")
                if not program_prefix:
                    continue
                tasks.append(
                    executor.submit(
                        lambda s=school_prefix, p=program_prefix: (
                            s,
                            p,
                            fetch_program_courses(term_code, s, p),
                        )
                    )
                )

        total = len(tasks)
        done = 0
        for fut in as_completed(tasks):
            s, p, courses = fut.result()
            courses_by_school.setdefault(s, {})[p] = courses
            done += 1
            logger.info("Fetched programs: %d/%d", done, total)

    ge_payloads: List[Tuple[str, str, Dict[str, Any]]] = []
    for ge_type, cat_prefix, letter in GE_CATEGORY_MAP:
        try:
            payload = fetch_ge(term_code, ge_type, cat_prefix)
            ge_payloads.append((ge_type, cat_prefix, payload))
        except Exception as e:
            logger.warning("GE fetch failed for %s/%s: %s", ge_type, cat_prefix, e)
            payload = None

        # Merge GE courses into main catalog and tag them (so they exist if absent)
        if payload:
            program_to_school = build_program_to_school(schools)
            for course in (payload or {}).get("courses", []):
                try:
                    scheduled = course.get("scheduledCourseCode") or {}
                    published = course.get("publishedCourseCode") or {}
                    matched = course.get("matchedCourseCode") or {}
                    prog_prefix = (
                        scheduled.get("prefix") or published.get("prefix") or matched.get("prefix")
                    )
                    school_prefix = program_to_school.get(prog_prefix)
                    if not prog_prefix or not school_prefix:
                        continue
                    grouped_list = aggregate_grouped_from_courses([course], preferred_prefix=prog_prefix)
                    dest = courses_by_school.setdefault(school_prefix, {}).setdefault(prog_prefix, [])
                    for g in grouped_list:
                        merge_group_into_target(dest, g, ge_tags=[letter])
                except Exception:
                    continue

    return schools, courses_by_school, ge_payloads


def aggregate_grouped_from_courses(course_list: List[Dict[str, Any]], preferred_prefix: Optional[str] = None) -> List[Dict[str, Any]]:
    aggregation: Dict[Tuple[Optional[str], Optional[str], Optional[str]], Dict[str, Any]] = {}
    for course in course_list or []:
        processed = _process_course(course, preferred_prefix=preferred_prefix)
        for item in processed:
            key = (item.get("title"), item.get("description"), item.get("courseCode"))
            if key not in aggregation:
                aggregation[key] = {
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "courseCode": item.get("courseCode"),
                    "sections": [],
                    "_seen": set(),
                }
            section_obj = item.get("section") or {}
            sec_code = section_obj.get("sectionCode")
            seen = aggregation[key]["_seen"]
            if sec_code and sec_code in seen:
                continue
            if sec_code:
                seen.add(sec_code)
            aggregation[key]["sections"].append(section_obj)
    result: List[Dict[str, Any]] = []
    for v in aggregation.values():
        v.pop("_seen", None)
        result.append(v)
    return result


def merge_group_into_target(target_list: List[Dict[str, Any]], grouped_item: Dict[str, Any], ge_tags: Optional[List[str]] = None) -> None:
    ge_tags = ge_tags or []
    t_title = grouped_item.get("title")
    t_desc = grouped_item.get("description")
    t_code = grouped_item.get("courseCode")
    found_index = None
    for idx, existing in enumerate(target_list or []):
        if (
            existing.get("title") == t_title
            and existing.get("description") == t_desc
            and existing.get("courseCode") == t_code
        ):
            found_index = idx
            break
    if found_index is None:
        new_item = dict(grouped_item)
        if ge_tags:
            new_item["GE"] = sorted(set(str(x) for x in ge_tags))
        target_list.append(new_item)
    else:
        existing = target_list[found_index]
        seen = set()
        for s in existing.get("sections", []) or []:
            c = s.get("sectionCode")
            if c:
                seen.add(c)
        for s in grouped_item.get("sections", []) or []:
            c = s.get("sectionCode")
            if c and c in seen:
                continue
            if c:
                seen.add(c)
            existing.setdefault("sections", []).append(s)
        if ge_tags:
            existing_ge = set(existing.get("GE") or [])
            for t in ge_tags:
                existing_ge.add(str(t))
            existing["GE"] = sorted(existing_ge)


