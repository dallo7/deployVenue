import psycopg2
import psycopg2.extras
import json
import os
import sys
from datetime import datetime, timedelta, timezone

# --- Database Connection Details ---
DB_HOST = os.getenv("DB_HOST", "beatbnk-db.cdgq4essi2q1.ap-southeast-2.rds.amazonaws.com")
DB_NAME = os.getenv("DB_NAME", "beatbnk_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASS", "X1SOrzeSrk")
DB_PORT = os.getenv("DB_PORT", "5432")


# --- Helper Functions for Venue Report ---

def get_venue_id_by_name(venue_name: str) -> int | None:
    """Finds a venue's unique ID by its name."""
    conn = None
    venue_id = None
    print(f"\n🔎 Searching for Venue ID for: '{venue_name}'...")
    sql = """
        SELECT id FROM venues WHERE "venueName" = %(venue_name)s LIMIT 1;
    """
    try:
        print(f"  - Connecting to database '{DB_NAME}'...")
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print(f"  - Executing query to find venue '{venue_name}'...")
        cur.execute(sql, {'venue_name': venue_name})
        result = cur.fetchone()
        if result:
            venue_id = result['id']
            print(f"  ✅ Found Venue ID: {venue_id}")
        else:
            print(f"  ⚠️  Warning: No venue found with the name '{venue_name}'.")
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"  ❌ An unexpected database error occurred: {error}")
        return None
    finally:
        if conn is not None:
            conn.close()
            print("  - Database connection closed.")
    return venue_id


def get_venue_header_stats(cur, venue_id: int, venue_user_id: int) -> dict:
    """Gets the main stats for the venue header."""
    sql = """
        SELECT
            (SELECT "venueName" FROM venues WHERE id = %(venue_id)s) AS venue_name,
            (SELECT "profileImageUrl" FROM venues WHERE id = %(venue_id)s) as venue_image_url,
            (SELECT COUNT(*) FROM follows WHERE "entityId" = %(user_id)s AND "entityType" = 'VENUE' AND "deletedAt" IS NULL) AS total_followers,
            (SELECT COUNT(*) FROM user_venue_bookings WHERE "venueId" = %(venue_id)s) AS total_bookings,
            (SELECT COUNT(*) FROM user_venue_bookings WHERE "venueId" = %(venue_id)s AND UPPER(status::text) = 'COMPLETED') AS completed_bookings;
    """
    cur.execute(sql, {'venue_id': venue_id, 'user_id': venue_user_id})
    result = cur.fetchone()
    return dict(result) if result else {}


def get_venue_time_filtered_stats(cur, venue_id: int, venue_user_id: int, start_date: datetime,
                                  end_date: datetime) -> dict:
    """Calculates time-filtered stats like new followers and booking requests."""
    sql = """
        SELECT
            (SELECT COUNT(*) FROM follows WHERE "entityId" = %(user_id)s AND "createdAt" BETWEEN %(start_date)s AND %(end_date)s) AS new_followers,
            (SELECT COUNT(*) FROM follows WHERE "entityId" = %(user_id)s AND "deletedAt" BETWEEN %(start_date)s AND %(end_date)s) AS unfollows,
            COUNT(id) FILTER (WHERE UPPER(status::text) = 'APPROVED') AS accepted_booking_requests,
            COUNT(id) FILTER (WHERE UPPER(status::text) = 'REJECTED') AS declined_booking_requests
        FROM venue_bookings
        WHERE "venueId" = %(venue_id)s AND "createdAt" BETWEEN %(start_date)s AND %(end_date)s;
    """
    params = {'venue_id': venue_id, 'user_id': venue_user_id, 'start_date': start_date, 'end_date': end_date}
    cur.execute(sql, params)
    result = cur.fetchone()
    return dict(result) if result else {}


def get_booking_requests_by_gender(cur, venue_id: int, months: int = 6) -> list:
    """Gets booking counts per month, segmented by user gender."""
    start_date = datetime.now(timezone.utc) - timedelta(days=months * 30)
    sql = """
        SELECT
            TO_CHAR(DATE_TRUNC('month', uvb."createdAt"), 'YYYY-MM') AS month,
            COUNT(uvb.id) FILTER (WHERE UPPER(u.gender::text) = 'MALE') AS male_bookings,
            COUNT(uvb.id) FILTER (WHERE UPPER(u.gender::text) = 'FEMALE') AS female_bookings
        FROM user_venue_bookings uvb
        JOIN users u ON uvb."userId" = u.id
        WHERE uvb."venueId" = %(venue_id)s AND uvb."createdAt" >= %(start_date)s
        GROUP BY month
        ORDER BY month;
    """
    cur.execute(sql, {'venue_id': venue_id, 'start_date': start_date})
    return [dict(row) for row in cur.fetchall()]


def get_popular_event_types(cur, venue_id: int, limit: int = 5) -> list:
    """Finds the most popular event categories hosted at the venue."""
    sql = """
        WITH total_events AS (
            SELECT COUNT(*) AS total FROM events WHERE "venueId" = %(venue_id)s
        )
        SELECT
            c.name AS category_name,
            COUNT(e.id) AS event_count,
            ROUND((COUNT(e.id) * 100.0) / NULLIF((SELECT total FROM total_events), 0), 2) AS percentage
        FROM events e
        JOIN category_mappings cm ON e.id = cm."eventId"
        JOIN categories c ON cm."categoryId" = c.id
        WHERE e."venueId" = %(venue_id)s
        GROUP BY c.name
        ORDER BY event_count DESC
        LIMIT %(limit)s;
    """
    cur.execute(sql, {'venue_id': venue_id, 'limit': limit})
    return [dict(row) for row in cur.fetchall()]


def get_top_clients(cur, venue_id: int, limit: int = 5) -> list:
    """Finds top clients by the number of bookings they've made."""
    sql = """
        SELECT
            u.name AS client_name,
            u."profileImageUrl",
            COUNT(uvb.id) AS booking_count
        FROM user_venue_bookings uvb
        JOIN users u ON uvb."userId" = u.id
        WHERE uvb."venueId" = %(venue_id)s
        GROUP BY u.id, u.name, u."profileImageUrl"
        ORDER BY booking_count DESC
        LIMIT %(limit)s;
    """
    cur.execute(sql, {'venue_id': venue_id, 'limit': limit})
    return [dict(row) for row in cur.fetchall()]


# --- Main Orchestration Function for Venues ---

def generate_venue_analytics_report(venue_id: int) -> dict:
    """Connects to the database and builds a complete analytics report for a single venue."""
    conn = None
    report = {}
    print("🚀 Starting venue analytics report generation...")

    try:
        print(f"  - Attempting to connect to database '{DB_NAME}'...")
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print("  ✅ Database connection successful.\n")

        print(f"  🔎 Fetching user ID for Venue ID: {venue_id}...")
        cur.execute('SELECT "userId" FROM venues WHERE id = %(id)s', {'id': venue_id})
        result = cur.fetchone()
        if not result:
            return {"error": f"Venue with ID {venue_id} not found."}
        venue_user_id = result['userId']
        print(f"  ✅ Found associated User ID: {venue_user_id}\n")

        print("--- Fetching Report Components ---")

        print("  📊 Fetching header stats...")
        header_stats = get_venue_header_stats(cur, venue_id, venue_user_id)
        print("  ...done.")

        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=30)
        print(f"  📈 Fetching time-filtered stats...")
        time_filtered_stats = get_venue_time_filtered_stats(cur, venue_id, venue_user_id, start_date, end_date)
        print("  ...done.")

        print("  👫 Fetching booking requests by gender...")
        bookings_by_gender = get_booking_requests_by_gender(cur, venue_id)
        print("  ...done.")

        print("  🎉 Fetching popular event types...")
        popular_event_types = get_popular_event_types(cur, venue_id)
        print("  ...done.")

        print("  ❤️  Fetching top clients...")
        top_clients = get_top_clients(cur, venue_id)
        print("  ...done.\n")

        print("--- Assembling Final Report ---")
        report = {
            "generated_at": end_date.isoformat(),
            "venue_id": venue_id,
            "venue_info": {"name": header_stats.get('venue_name'),
                           "profile_image_url": header_stats.get('venue_image_url')},
            "header_stats": {"total_followers": header_stats.get('total_followers'),
                             "total_bookings": header_stats.get('total_bookings'),
                             "completed_bookings": header_stats.get('completed_bookings')},
            "analytics_page": {"period": f"{start_date.date()} to {end_date.date()}", "stats": time_filtered_stats},
            "charts": {"bookings_by_gender_over_time": bookings_by_gender, "popular_event_types": popular_event_types},
            "top_clients": top_clients
        }
        print("  ✅ Final report dictionary assembled successfully.\n")
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ An unexpected error occurred: {error}")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()
            print("🚪 Database connection closed.")

    return report


# --- Example Usage ---

if __name__ == '__main__':
    target_venue_name = "RocoMamas Kenya"
    print("=====================================================")
    print(f"   Starting Full Analytics Run for Venue: {target_venue_name}")
    print("=====================================================")

    venue_id = get_venue_id_by_name(target_venue_name)

    if venue_id:
        analytics_data = generate_venue_analytics_report(venue_id)
        print("\n=====================================================")
        if "error" in analytics_data:
            print("   ❌ Report generation failed.")
        else:
            print("   ✅ Report generation complete. Final JSON output:")
            print("=====================================================")
            json_output = json.dumps(analytics_data, indent=4, default=str)
            print(json_output)
    else:
        print(f"\nCould not proceed with report generation as venue '{target_venue_name}' was not found.")