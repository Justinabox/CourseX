DROP DATABASE coursex;

CREATE DATABASE IF NOT EXISTS coursex
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE coursex;

-- ============================================================================
-- CORE TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table: semesters
-- Description: Stores semester metadata and tracks active semester
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS semesters (
    semester_id INT NOT NULL UNIQUE PRIMARY KEY COMMENT 'e.g., 20251 for Spring 2025',
    semester_name VARCHAR(50) NOT NULL COMMENT 'e.g., Spring 2025',
    year INT NOT NULL,
    term VARCHAR(20) NOT NULL COMMENT 'Spring, Fall, Summer',
    is_active BOOLEAN DEFAULT FALSE COMMENT 'Only one semester should be active at a time',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_semester_id (semester_id),
    INDEX idx_active (is_active),
    INDEX idx_year_term (year, term)
) COMMENT='Semester metadata and active semester tracking';

-- ----------------------------------------------------------------------------
-- Table: schools
-- Description: Stores school/college information
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schools (
    school_id VARCHAR(4) NOT NULL UNIQUE PRIMARY KEY COMMENT 'e.g., DRNS, FINE, BUS',
    school_name VARCHAR(128) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_school_id (school_id)
) COMMENT='Schools and colleges within the university';

-- ----------------------------------------------------------------------------
-- Table: programs
-- Description: Stores academic programs/departments within schools
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS programs (
    school_id VARCHAR(4) NOT NULL,
    program_id VARCHAR(4) NOT NULL PRIMARY KEY COMMENT 'e.g., COLT, AMST, ANTH',
    program_name VARCHAR(128) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (school_id) REFERENCES schools(school_id) ON DELETE CASCADE,
    INDEX idx_school_id (school_id),
    INDEX idx_program_id (program_id)
) COMMENT='Academic programs and departments';

-- ----------------------------------------------------------------------------
-- Table: professors
-- Description: Stores professor information and RateMyProfessor ratings
-- Note: This is global data, not semester-specific
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS professors (
    professor_name VARCHAR(128) NOT NULL UNIQUE PRIMARY KEY,
    rmp_id INT COMMENT 'RateMyProfessor ID',
    difficulty DECIMAL(3,2) COMMENT 'Difficulty rating (0.00-5.00)',
    rating DECIMAL(3,2) COMMENT 'Overall rating (0.00-5.00)',
    rating_count INT COMMENT 'Number of ratings',
    take_again_percentage DECIMAL(5,2) COMMENT 'Percentage who would take again',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_rmp_id (rmp_id),
    INDEX idx_rating (rating),
    INDEX idx_difficulty (difficulty),
    INDEX idx_rating_count (rating_count),
    INDEX idx_take_again_percentage (take_again_percentage)
) COMMENT='Professor information and RateMyProfessor ratings';

-- ----------------------------------------------------------------------------
-- Table: courses
-- Description: Stores course master data (semester-specific)
-- TiDB Serverless Note: NO FULLTEXT INDEX support - using prefix indexes
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS courses (
    semester_id INT NOT NULL,
    course_id VARCHAR(16) NOT NULL COMMENT 'e.g., COLT-102 or COLT-102-40386 (section ID if custom created course)',
    program_id VARCHAR(4) NOT NULL,
    course_number INT NOT NULL COMMENT 'e.g., 102',
    title VARCHAR(256) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (semester_id, course_id),
    FOREIGN KEY (semester_id) REFERENCES semesters(semester_id) ON DELETE CASCADE,
    FOREIGN KEY (program_id) REFERENCES programs(program_id) ON DELETE CASCADE,
    INDEX idx_course_number (course_number),
    INDEX idx_semester_id (semester_id),
    INDEX idx_program_id (program_id),
    INDEX idx_semester_program (semester_id, program_id, course_number),
    INDEX idx_semester_course (semester_id, course_id),
    INDEX idx_title (title(255)),
    INDEX idx_description (description(500))
) COMMENT='Course master data with title and description';

-- ----------------------------------------------------------------------------
-- Table: sections
-- Description: Stores course section information (semester-specific)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sections (
    semester_id INT NOT NULL,
    section_id INT NOT NULL,
    course_id VARCHAR(16) NOT NULL,
    section_type ENUM('Lecture', 'Discussion', 'Lab', 'Quiz', 'Studio', 'Other') NOT NULL,
    units VARCHAR(16) NOT NULL,
    total_seats INT NOT NULL,
    registered_seats INT NOT NULL,
    location VARCHAR(64),
    time_schedule VARCHAR(64),
    d_clearance_required BOOLEAN DEFAULT FALSE COMMENT 'Department clearance required',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (semester_id, section_id),
    FOREIGN KEY (semester_id, course_id) REFERENCES courses(semester_id, course_id) ON DELETE CASCADE,
    FOREIGN KEY (semester_id) REFERENCES semesters(semester_id) ON DELETE CASCADE,
    INDEX idx_course_id (course_id),
    INDEX idx_section_type (section_type),
    INDEX idx_units (units)
) COMMENT='Course sections with enrollment and schedule information';

-- ----------------------------------------------------------------------------
-- Table: section_instructors
-- Description: Junction table for section-professor many-to-many relationship
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS section_instructors (
    semester_id INT NOT NULL,
    section_id INT NOT NULL,
    professor_name VARCHAR(128) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (semester_id, section_id) REFERENCES sections(semester_id, section_id) ON DELETE CASCADE,
    FOREIGN KEY (professor_name) REFERENCES professors(professor_name) ON DELETE CASCADE,
    UNIQUE KEY uk_semester_section_professor (semester_id, section_id, professor_name),
    INDEX idx_semester_id (semester_id),
    INDEX idx_section_id (section_id),
    INDEX idx_professor_name (professor_name)
) COMMENT='Links sections to their instructors';

-- ----------------------------------------------------------------------------
-- Table: course_ge_categories
-- Description: Stores General Education category assignments
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS course_ge_categories (
    semester_id INT NOT NULL,
    course_id VARCHAR(16) NOT NULL,
    ge_category ENUM('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H') NOT NULL COMMENT 'e.g., A, B, C, D, E, F, G, H',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (semester_id, course_id) REFERENCES courses(semester_id, course_id) ON DELETE CASCADE,
    UNIQUE KEY uk_semester_course_ge (semester_id, course_id, ge_category),
    INDEX idx_semester_id (semester_id),
    INDEX idx_course_id (course_id),
    INDEX idx_ge_category (ge_category)
) COMMENT='General Education category assignments for courses';

-- ----------------------------------------------------------------------------
-- Table: course_prerequisites
-- Description: Stores course prerequisites
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS section_prerequisites (
    semester_id INT NOT NULL,
    section_id INT NOT NULL,
    prerequisite_text VARCHAR(128) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (semester_id, section_id) REFERENCES sections(semester_id, section_id) ON DELETE CASCADE,
    UNIQUE KEY uk_semester_section_prerequisite (semester_id, section_id, prerequisite_text),
    INDEX idx_semester_id (semester_id),
    INDEX idx_section_id (section_id),
    INDEX idx_semester_section (semester_id, section_id)
) COMMENT='Course prerequisite requirements';

-- ----------------------------------------------------------------------------
-- Table: section_duplicated_credits
-- Description: Stores duplicated credit information for sections
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS section_duplicated_credits (
    semester_id INT NOT NULL,
    section_id INT NOT NULL,
    duplicated_text VARCHAR(128) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (semester_id, section_id) REFERENCES sections(semester_id, section_id) ON DELETE CASCADE,
    UNIQUE KEY uk_semester_section_duplicated_credit (semester_id, section_id, duplicated_text),
    INDEX idx_semester_id (semester_id),
    INDEX idx_section_id (section_id),
    INDEX idx_semester_section (semester_id, section_id)
) COMMENT='Courses that provide duplicated credit';

-- ----------------------------------------------------------------------------
-- Table: etl_runs
-- Description: Tracks ETL executions and outcomes
-- ----------------------------------------------------------------------------
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
) COMMENT='ETL execution audit and metrics';

-- ============================================================================
-- STAGING TABLES FOR HOURLY REFRESH
-- ============================================================================

DROP TABLE IF EXISTS staging_courses;
DROP TABLE IF EXISTS staging_sections;
DROP TABLE IF EXISTS staging_section_instructors;
DROP TABLE IF EXISTS staging_course_ge_categories;
DROP TABLE IF EXISTS staging_section_prerequisites;
DROP TABLE IF EXISTS staging_section_duplicated_credits;
DROP TABLE IF EXISTS staging_schools;
DROP TABLE IF EXISTS staging_programs;
DROP TABLE IF EXISTS staging_professors;

CREATE TABLE staging_courses LIKE courses;
CREATE TABLE staging_sections LIKE sections;
CREATE TABLE staging_section_instructors LIKE section_instructors;
CREATE TABLE staging_course_ge_categories LIKE course_ge_categories;
CREATE TABLE staging_section_prerequisites LIKE section_prerequisites;
CREATE TABLE staging_section_duplicated_credits LIKE section_duplicated_credits;
CREATE TABLE staging_schools LIKE schools;
CREATE TABLE staging_programs LIKE programs;
CREATE TABLE staging_professors LIKE professors;

CREATE OR REPLACE VIEW v_course_search AS
SELECT
    c.semester_id,
    c.course_id,
    c.title,
    c.description,
    c.program_id,
    c.course_number,
    s.section_id,
    s.section_type,
    s.units,
    s.total_seats,
    s.registered_seats,
    s.location,
    s.time_schedule,
    s.d_clearance_required,
    sp.prerequisite_text,
    sdc.duplicated_text
FROM courses c
LEFT JOIN sections s ON c.semester_id = s.semester_id AND c.course_id = s.course_id
LEFT JOIN (
    SELECT semester_id, section_id,
           GROUP_CONCAT(prerequisite_text SEPARATOR ', ') AS prerequisite_text
    FROM section_prerequisites
    GROUP BY semester_id, section_id
) sp ON s.semester_id = sp.semester_id AND s.section_id = sp.section_id
LEFT JOIN (
    SELECT semester_id, section_id,
           GROUP_CONCAT(duplicated_text SEPARATOR ', ') AS duplicated_text
    FROM section_duplicated_credits
    GROUP BY semester_id, section_id
) sdc ON s.semester_id = sdc.semester_id AND s.section_id = sdc.section_id
ORDER BY c.course_number, s.section_id;