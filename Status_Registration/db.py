import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch

DB_URL = os.getenv("DB_URL")
SCHEMA = "public"
CLASSES_TABLE = "classes"
TIMECHART_TABLE = "timechart"

def save_registrations(data):
    if not DB_URL:
        raise ValueError("DATABASE_URL environment variable not set")
    
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            # Step 1: Create temporary tables
            cur.execute("""
                CREATE TEMP TABLE temp_classes (
                    code TEXT,
                    name TEXT,
                    tags TEXT
                ) ON COMMIT DROP
            """)
            
            cur.execute("""
                CREATE TEMP TABLE temp_timechart (
                    code TEXT,
                    name TEXT,
                    class_group BIGINT,
                    type TEXT,
                    day TEXT,
                    time_from TIME,
                    time_to TIME,
                    class_size BIGINT,
                    waiting BIGINT,
                    status BOOLEAN,
                    location TEXT
                ) ON COMMIT DROP
            """)
            
            # Step 2: Extract unique classes and insert into temp_classes
            unique_classes = {}
            for row in data:
                key = (row.get("Code"), row.get("Name"), row.get("Tags"))
                if key not in unique_classes:
                    unique_classes[key] = True
            
            class_rows = [
                (code, name, tags)
                for code, name, tags in unique_classes.keys()
            ]
            
            if class_rows:
                execute_batch(
                    cur,
                    "INSERT INTO temp_classes (code, name, tags) VALUES (%s, %s, %s)",
                    class_rows
                )
            
            # Step 3: Insert all timechart data into temp_timechart
            timechart_rows = [
                (
                    row.get("Code"),
                    row.get("Name"),
                    row.get("Group"),
                    row.get("Type"),
                    row.get("Day"),
                    row.get("From") or None,
                    row.get("To") or None,
                    row.get("Class Size") or None,
                    row.get("Waiting") or None,
                    row.get("Status"),
                    row.get("Location")
                )
                for row in data
            ]
            
            insert_temp_sql = """
                INSERT INTO temp_timechart (
                    code, name, class_group, type, day,
                    time_from, time_to, class_size,
                    waiting, status, location
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s::TIME, %s::TIME,
                    NULLIF(%s, '')::BIGINT,
                    NULLIF(%s, '')::BIGINT,
                    %s, %s
                )
            """
            
            if timechart_rows:
                execute_batch(cur, insert_temp_sql, timechart_rows)
            
            # Step 4: Sync classes table
            # Insert new classes
            cur.execute(sql.SQL("""
                INSERT INTO {schema}.{table} (code, name, tags)
                SELECT t.code, t.name, t.tags
                FROM temp_classes AS t
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM {schema}.{table} AS c
                    WHERE c.code = t.code 
                    AND c.name = t.name 
                    AND c.tags = t.tags
                )
                RETURNING id, code, name
            """).format(
                schema=sql.Identifier(SCHEMA),
                table=sql.Identifier(CLASSES_TABLE)
            ))
            
            inserted_classes = cur.fetchall()
            inserted_classes_count = len(inserted_classes)
            
            # Delete classes that no longer exist
            cur.execute(sql.SQL("""
                DELETE FROM {schema}.{table} AS c
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM temp_classes AS t
                    WHERE c.code = t.code 
                    AND c.name = t.name 
                    AND c.tags = t.tags
                )
                RETURNING id, code, name
            """).format(
                schema=sql.Identifier(SCHEMA),
                table=sql.Identifier(CLASSES_TABLE)
            ))
            
            deleted_classes = cur.fetchall()
            deleted_classes_count = len(deleted_classes)
            
            # Step 5: Sync timechart table
            # First, capture updates before they happen
            cur.execute(sql.SQL("""
                SELECT 
                    tc.id,
                    c.code,
                    c.name,
                    tc.class_group,
                    tc.waiting AS old_waiting,
                    t.waiting AS new_waiting,
                    tc.status AS old_status,
                    t.status AS new_status
                FROM {schema}.{timechart_table} AS tc
                JOIN {schema}.{classes_table} AS c ON tc.class_id = c.id
                JOIN temp_timechart AS t ON (
                    c.code = t.code
                    AND c.name = t.name
                    AND tc.class_group = t.class_group
                    AND tc.type = t.type
                    AND tc.day = t.day
                    AND tc.time_from = t.time_from
                    AND tc.time_to = t.time_to
                    AND COALESCE(tc.class_size, 0) = COALESCE(t.class_size, 0)
                    AND tc.location = t.location
                )
                WHERE (
                    COALESCE(tc.waiting, 0) != COALESCE(t.waiting, 0)
                    OR tc.status != t.status
                )
            """).format(
                schema=sql.Identifier(SCHEMA),
                timechart_table=sql.Identifier(TIMECHART_TABLE),
                classes_table=sql.Identifier(CLASSES_TABLE)
            ))
            
            updated_rows = cur.fetchall()
            
            # Perform the update
            cur.execute(sql.SQL("""
                UPDATE {schema}.{timechart_table} AS tc
                SET 
                    waiting = t.waiting,
                    status = t.status
                FROM temp_timechart AS t
                JOIN {schema}.{classes_table} AS c ON (
                    c.code = t.code
                    AND c.name = t.name
                )
                WHERE 
                    tc.class_id = c.id
                    AND tc.class_group = t.class_group
                    AND tc.type = t.type
                    AND tc.day = t.day
                    AND tc.time_from = t.time_from
                    AND tc.time_to = t.time_to
                    AND COALESCE(tc.class_size, 0) = COALESCE(t.class_size, 0)
                    AND tc.location = t.location
                    AND (
                        COALESCE(tc.waiting, 0) != COALESCE(t.waiting, 0)
                        OR tc.status != t.status
                    )
            """).format(
                schema=sql.Identifier(SCHEMA),
                timechart_table=sql.Identifier(TIMECHART_TABLE),
                classes_table=sql.Identifier(CLASSES_TABLE)
            ))
            
            updated_count = len(updated_rows)
            
            # Insert new timechart entries
            cur.execute(sql.SQL("""
                INSERT INTO {schema}.{timechart_table} (
                    class_id, class_group, type, day,
                    time_from, time_to, class_size,
                    waiting, status, location
                )
                SELECT 
                    c.id, t.class_group, t.type, t.day,
                    t.time_from, t.time_to, t.class_size,
                    t.waiting, t.status, t.location
                FROM temp_timechart AS t
                JOIN {schema}.{classes_table} AS c ON (
                    c.code = t.code
                    AND c.name = t.name
                )
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM {schema}.{timechart_table} AS tc
                    WHERE 
                        tc.class_id = c.id
                        AND tc.class_group = t.class_group
                        AND tc.type = t.type
                        AND tc.day = t.day
                        AND tc.time_from = t.time_from
                        AND tc.time_to = t.time_to
                        AND COALESCE(tc.class_size, 0) = COALESCE(t.class_size, 0)
                        AND tc.location = t.location
                )
                RETURNING id, class_id, class_group
            """).format(
                schema=sql.Identifier(SCHEMA),
                timechart_table=sql.Identifier(TIMECHART_TABLE),
                classes_table=sql.Identifier(CLASSES_TABLE)
            ))
            
            inserted_timechart = cur.fetchall()
            inserted_timechart_count = len(inserted_timechart)
            
            # Delete timechart entries that no longer exist
            cur.execute(sql.SQL("""
                DELETE FROM {schema}.{timechart_table} AS tc
                USING {schema}.{classes_table} AS c
                WHERE tc.class_id = c.id
                AND NOT EXISTS (
                    SELECT 1 
                    FROM temp_timechart AS t
                    WHERE 
                        c.code = t.code
                        AND c.name = t.name
                        AND tc.class_group = t.class_group
                        AND tc.type = t.type
                        AND tc.day = t.day
                        AND tc.time_from = t.time_from
                        AND tc.time_to = t.time_to
                        AND COALESCE(tc.class_size, 0) = COALESCE(t.class_size, 0)
                        AND tc.location = t.location
                )
                RETURNING tc.id, c.code, c.name, tc.class_group
            """).format(
                schema=sql.Identifier(SCHEMA),
                timechart_table=sql.Identifier(TIMECHART_TABLE),
                classes_table=sql.Identifier(CLASSES_TABLE)
            ))
            
            deleted_timechart = cur.fetchall()
            deleted_timechart_count = len(deleted_timechart)
            
            # Print summary
            print(f"✅ Database sync complete:")
            print(f"\n📚 CLASSES TABLE:")
            print(f"   ➕ Inserted: {inserted_classes_count} new classes")
            if inserted_classes:
                for row_id, code, name in inserted_classes[:5]:
                    print(f"      • ID {row_id}: {code} - {name}")
                if len(inserted_classes) > 5:
                    print(f"      ... and {len(inserted_classes) - 5} more")
            
            print(f"   ➖ Deleted: {deleted_classes_count} classes")
            if deleted_classes:
                for row_id, code, name in deleted_classes[:5]:
                    print(f"      • ID {row_id}: {code} - {name}")
                if len(deleted_classes) > 5:
                    print(f"      ... and {len(deleted_classes) - 5} more")
            
            print(f"\n📅 TIMECHART TABLE:")
            print(f"   📝 Updated: {updated_count} entries")
            if updated_rows:
                for row in updated_rows[:5]:
                    row_id, code, name, group = row[0], row[1], row[2], row[3]
                    old_waiting, new_waiting = row[4], row[5]
                    old_status, new_status = row[6], row[7]
                    
                    changes = []
                    if old_waiting != new_waiting:
                        changes.append(f"waiting: {old_waiting} → {new_waiting}")
                    if old_status != new_status:
                        status_old = "open" if old_status else "closed"
                        status_new = "open" if new_status else "closed"
                        changes.append(f"status: {status_old} → {status_new}")
                    
                    print(f"      • ID {row_id}: {code} - {name} (Group {group})")
                    print(f"        Changes: {', '.join(changes)}")
                if len(updated_rows) > 5:
                    print(f"      ... and {len(updated_rows) - 5} more")
            
            print(f"   ➕ Inserted: {inserted_timechart_count} new entries")
            if inserted_timechart:
                for row in inserted_timechart[:5]:
                    print(f"      • ID {row[0]}: Class ID {row[1]} (Group {row[2]})")
                if len(inserted_timechart) > 5:
                    print(f"      ... and {len(inserted_timechart) - 5} more")
            
            print(f"   ➖ Deleted: {deleted_timechart_count} stale entries")
            if deleted_timechart:
                for row_id, code, name, group in deleted_timechart[:5]:
                    print(f"      • ID {row_id}: {code} - {name} (Group {group})")
                if len(deleted_timechart) > 5:
                    print(f"      ... and {len(deleted_timechart) - 5} more")