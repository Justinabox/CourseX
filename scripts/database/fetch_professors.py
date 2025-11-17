import json
import time
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)


GRAPHQL_URL = "https://www.ratemyprofessors.com/graphql"
SCHOOL_ID = "U2Nob29sLTEzODE="  # USC
BATCH_SIZE = 1000


GRAPHQL_QUERY = """query TeacherSearchResultsPageQuery(
  $query: TeacherSearchQuery!
  $schoolID: ID
  $includeSchoolFilter: Boolean!
) {
  search: newSearch {
    ...TeacherSearchPagination_search_2MvZSr
  }
  school: node(id: $schoolID) @include(if: $includeSchoolFilter) {
    __typename
    ... on School {
      name
      ...StickyHeaderContent_school
    }
    id
  }
}

fragment CardFeedback_teacher on Teacher {
  wouldTakeAgainPercent
  avgDifficulty
}

fragment CardName_teacher on Teacher {
  firstName
  lastName
}

fragment CardSchool_teacher on Teacher {
  department
  school {
    name
    id
  }
}

fragment CompareSchoolLink_school on School {
  legacyId
}

fragment HeaderDescription_school on School {
  name
  city
  state
  legacyId
  ...RateSchoolLink_school
  ...CompareSchoolLink_school
}

fragment HeaderRateButton_school on School {
  ...RateSchoolLink_school
  ...CompareSchoolLink_school
}

fragment RateSchoolLink_school on School {
  legacyId
}

fragment StickyHeaderContent_school on School {
  name
  ...HeaderDescription_school
  ...HeaderRateButton_school
}

fragment TeacherBookmark_teacher on Teacher {
  id
  isSaved
}

fragment TeacherCard_teacher on Teacher {
  id
  legacyId
  avgRating
  numRatings
  ...CardFeedback_teacher
  ...CardSchool_teacher
  ...CardName_teacher
  ...TeacherBookmark_teacher
}

fragment TeacherSearchPagination_search_2MvZSr on newSearch {
  teachers(query: $query, first: 1000, after: "") {
    didFallback
    edges {
      cursor
      node {
        ...TeacherCard_teacher
        id
        __typename
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
    resultCount
    filters {
      field
      options {
        value
        id
      }
    }
  }
}
"""


def _make_request(cursor: str = "") -> Dict[str, Any]:
    query = GRAPHQL_QUERY.replace('first: 1000, after: ""', f'first: {BATCH_SIZE}, after: "{cursor}"')
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Referer": "https://www.ratemyprofessors.com/search/professors/1381?q=*",
        "Origin": "https://www.ratemyprofessors.com",
        "Content-Type": "application/json",
        "Authorization": "null",
    }
    payload = {
        "query": query,
        "variables": {
            "query": {"text": "", "schoolID": SCHOOL_ID, "fallback": True},
            "schoolID": SCHOOL_ID,
            "includeSchoolFilter": True,
        },
    }
    resp = requests.post(GRAPHQL_URL, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()


def fetch_professors_rows() -> List[Tuple[str, Optional[int], Optional[float], Optional[float], Optional[int], Optional[float]]]:
    """
    Return rows for table 'professors' with columns:
    (professor_name, rmp_id, difficulty, rating, rating_count, take_again_percentage)
    """
    professors: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    cursor = ""
    page = 0
    while True:
        data = _make_request(cursor)
        teachers_data = data.get("data", {}).get("search", {}).get("teachers", {})
        edges = teachers_data.get("edges", [])
        page_info = teachers_data.get("pageInfo", {})
        page += 1
        logger.info("RMP page %d: fetched %d", page, len(edges))

        for edge in edges:
            node = edge.get("node", {})
            full_name = f"{node.get('firstName', '')} {node.get('lastName', '')}".strip()
            professors[full_name].append(
                {
                    "id": node.get("legacyId"),
                    "difficulty": node.get("avgDifficulty"),
                    "rating": node.get("avgRating"),
                    "rating_count": node.get("numRatings"),
                    "take_again": node.get("wouldTakeAgainPercent"),
                }
            )

        if not page_info.get("hasNextPage", False):
            break
        cursor = page_info.get("endCursor", "")
        if not cursor:
            break
        time.sleep(1)

    rows: List[Tuple[str, Optional[int], Optional[float], Optional[float], Optional[int], Optional[float]]] = []
    for name, entries in professors.items():
        if not entries:
            continue
        if len(entries) == 1:
            e = entries[0]
            rows.append(
                (
                    name,
                    e.get("id"),
                    e.get("difficulty"),
                    e.get("rating"),
                    e.get("rating_count"),
                    e.get("take_again"),
                )
            )
        else:
            diffs = [e.get("difficulty") for e in entries if e.get("difficulty") is not None]
            rats = [e.get("rating") for e in entries if e.get("rating") is not None]
            counts = [e.get("rating_count") for e in entries if e.get("rating_count") is not None]
            takes = [e.get("take_again") for e in entries if e.get("take_again") is not None]
            rows.append(
                (
                    name,
                    None,
                    round(sum(diffs) / len(diffs), 2) if diffs else None,
                    round(sum(rats) / len(rats), 2) if rats else None,
                    int(sum(counts) / len(counts)) if counts else None,
                    round(sum(takes) / len(takes), 2) if takes else None,
                )
            )
    return rows

# Professors with duplicated names seems like they are overridden by the last one.
