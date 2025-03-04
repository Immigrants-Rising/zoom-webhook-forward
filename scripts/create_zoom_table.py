import os
import sys
import argparse
from dotenv import load_dotenv
from pyairtable import Api


def main():
    parser = argparse.ArgumentParser(
        description="Create an Airtable table for meeting events"
    )
    parser.add_argument("--base-id", required=True, help="Airtable base ID")
    parser.add_argument(
        "--table-name", default="Meeting Events", help="Name for the new table"
    )
    parser.add_argument(
        "--table-description",
        default="Table containing meeting event data",
        help="Description for the new table",
    )
    args = parser.parse_args()

    # Load environment variables from .env file
    load_dotenv()

    # Retrieve API key from environment
    AIRTABLE_ACCESS_TOKEN = os.getenv("AIRTABLE_ACCESS_TOKEN")
    if not AIRTABLE_ACCESS_TOKEN:
        print("Error: AIRTABLE_ACCESS_TOKEN is not set in the environment variables.")
        sys.exit(1)

    # Define fields
    fields = [
        {  # You can't make autonumber or formula fields via the API. Must be done manually.
            "name": "ID",
            "type": "singleLineText",  # Manually change to autonumber or formula
            "description": "Unique identifier for the record (auto-generated)",
        },
        {"name": "Event Type", "type": "singleLineText", "description": "`event`"},
        {
            "name": "Event Timestamp",
            "type": "number",
            "description": "`event_ts`",
            "options": {"precision": 0},
        },
        {
            "name": "Account ID",
            "type": "singleLineText",
            "description": "`payload.account_id`",
        },
        {
            "name": "Meeting ID",
            "type": "singleLineText",
            "description": "`payload.object.id`",
        },
        {
            "name": "Meeting UUID",
            "type": "singleLineText",
            "description": "`payload.object.uuid`",
        },
        {
            "name": "Host ID",
            "type": "singleLineText",
            "description": "`payload.object.host_id`",
        },
        {
            "name": "Meeting Topic",
            "type": "singleLineText",
            "description": "`payload.object.topic`",
        },
        {
            "name": "Meeting Type",
            "type": "number",
            "description": "`payload.object.type`",
            "options": {"precision": 0},
        },
        {
            "name": "Start Time",
            "type": "dateTime",
            "description": "`payload.object.start_time`",
            "options": {
                "timeZone": "utc",
                "dateFormat": {"name": "iso"},
                "timeFormat": {"name": "24hour"},
            },
        },
        {
            "name": "Timezone",
            "type": "singleLineText",
            "description": "`payload.object.timezone`",
        },
        {
            "name": "Duration",
            "type": "number",
            "description": "`payload.object.duration`",
            "options": {"precision": 0},
        },
        {
            "name": "Participant User ID",
            "type": "singleLineText",
            "description": "`payload.object.participant.user_id`",
        },
        {
            "name": "Participant Name",
            "type": "singleLineText",
            "description": "`payload.object.participant.user_name`",
        },
        {
            "name": "Participant ID",
            "type": "singleLineText",
            "description": "`payload.object.participant.id`",
        },
        {
            "name": "Participant UUID",
            "type": "singleLineText",
            "description": "`payload.object.participant.participant_uuid`",
        },
        {
            "name": "Leave Time",
            "type": "dateTime",
            "description": "`payload.object.participant.leave_time`",
            "options": {
                "timeZone": "utc",
                "dateFormat": {"name": "iso"},
                "timeFormat": {"name": "24hour"},
            },
        },
        {
            "name": "Leave Reason",
            "type": "multilineText",
            "description": "`payload.object.participant.leave_reason`",
        },
        {
            "name": "Event Datetime",
            "type": "dateTime",
            "description": "`payload.object.participant.date_time`. `payload.object.participant.join_time` for `webinar.participant_joined` events",
            "options": {
                "timeZone": "utc",
                "dateFormat": {"name": "iso"},
                "timeFormat": {"name": "24hour"},
            },
        },
        {
            "name": "Email",
            "type": "email",
            "description": "`payload.object.participant.email`",
        },
        {
            "name": "Registrant ID",
            "type": "singleLineText",
            "description": "`payload.object.participant.registrant_id`",
        },
        {
            "name": "Participant User ID Alt",
            "type": "singleLineText",
            "description": """`payload.object.participant.participant_user_id`
            
            This seems to be the same as Participant ID (`payload.object.participant.id`). This is mostly the case, but for some reason, they are not the same for a small amount of records.""",
        },
        {
            "name": "Customer Key",
            "type": "singleLineText",
            "description": "`payload.object.participant.customer_key`",
        },
        {
            "name": "Phone Number",
            "type": "phoneNumber",
            "description": "`payload.object.participant.phone_number`",
        },
    ]

    # Initialize the Airtable API client
    api = Api(AIRTABLE_ACCESS_TOKEN)
    base = api.base(args.base_id)

    # Step 1: Create the table with all fields
    try:
        # Create the table with just the ID field first
        table = base.create_table(
            name=args.table_name,
            fields=[fields[0]],  # Just the ID field initially
            description=args.table_description,
        )
        print(f"Table '{args.table_name}' created successfully with an ID field.")

        # Add the remaining fields one by one
        for field in fields[1:]:
            try:
                field_name = field.get("name")
                field_type = field.get("type")
                field_description = field.get("description", None)
                field_options = field.get("options", None)

                # Use the table's create_field method
                table.create_field(
                    name=field_name,
                    field_type=field_type,
                    description=field_description,
                    options=field_options,
                )
                print(f"Field '{field_name}' added successfully.")
            except Exception as e:
                print(f"\nError: Failed to add field '{field_name}': {e}\n")
                print(f"Field length: {len(field_name)}")

        print(f"\nAll fields added to '{args.table_name}' table.")
    except Exception as e:
        print(f"\n\nError: Failed to create table '{args.table_name}': {e}\n\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
