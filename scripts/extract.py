# This script is useful for extracting Zoom Event Data from the error logs:
# Specifically for error type: "Error forwarding webhook: AxiosError: Request failed with status code 404"
# The JSON results can then be used to process events that failed to forward to the webhook.

import json
import re
import argparse
from typing import Dict, List, Tuple, Any, Optional

def extract_config_data(json_path: str, debug_mode: bool = False) -> Tuple[List[Dict], List[Dict], List[str], List]:
    """
    Extract the config.data values from the textPayload fields in a JSON file
    containing an array of objects. Only extracts entries with payload data.
    
    Args:
        json_path (str): Path to the JSON file
        debug_mode (bool): If True, prints additional debugging information
        
    Returns:
        Tuple containing:
        - List of successfully extracted config.data values
        - List of original objects that had a match
        - List of error messages for non-matching entries
    """
    try:
        # Read the JSON file
        with open(json_path, 'r') as file:
            data = json.load(file)
        
        # Handle both single object and array of objects
        if not isinstance(data, list):
            data = [data]
            
        successful_results = []
        matching_objects = []
        error_messages = []
        
        # If debug mode is on, examine the first few objects
        if debug_mode and len(data) > 0:
            print("\nDEBUG: Examining first object structure...")
            first_obj = data[0]
            if 'textPayload' in first_obj:
                text_sample = first_obj['textPayload'][:200] + "..." if len(first_obj['textPayload']) > 200 else first_obj['textPayload']
                print(f"DEBUG: textPayload sample: {text_sample}")
                
                # Check for data pattern
                if "data: " in text_sample:
                    index = text_sample.find("data: ")
                    print(f"DEBUG: Found 'data: ' at position {index}")
                    print(f"DEBUG: Content after 'data: ': {text_sample[index:index+50]}...")
        
        for i, item in enumerate(data):
            # Extract the textPayload
            text_payload = item.get('textPayload', '')
            
            if not text_payload:
                error_messages.append(f"Entry {i}: No textPayload field found")
                continue
                
            # Try multiple patterns to match the data field
            # Pattern 1: Standard format with single quotes
            match = re.search(r"data: '(\{\"payload\":.*?)'(?=\n|\r|$|\s*\})", text_payload)
            
            # Pattern 2: With double quotes
            if not match:
                match = re.search(r'data: "(\{\"payload\":.*?)"(?=\n|\r|$|\s*\})', text_payload)
            
            # Pattern 3: With backticks (found in your example)
            if not match:
                match = re.search(r"data: `(\{\"payload\":.*?)`(?=\n|\r|$|\s*\})", text_payload)
            
            # Pattern 4: More flexible with any JSON content
            if not match:
                match = re.search(r"data: ['`\"](\{.*?\})['`\"](?=\n|\r|$|\s*\})", text_payload)
            
            # Pattern 5: Extremely permissive match
            if not match:
                match = re.search(r"data: ['`\"](.+?)['`\"]", text_payload)
            
            if match:
                config_data = match.group(1)
                # If the data is JSON, we can parse it
                try:
                    # Handle possible escaping in the JSON string
                    if config_data.startswith('{\\\"'):
                        # Double escaped quotes
                        cleaned_data = config_data.replace('\\"', '"').replace('\\\\', '\\')
                    else:
                        # Normal JSON
                        cleaned_data = config_data
                        
                    parsed_data = json.loads(cleaned_data)
                    
                    # Only include if it has a payload field
                    if isinstance(parsed_data, dict) and 'payload' in parsed_data:
                        successful_results.append(parsed_data)
                        matching_objects.append(item)
                    else:
                        error_msg = f"Entry {i}: Matched data doesn't contain 'payload' field"
                        error_messages.append(error_msg)
                        
                except json.JSONDecodeError as e:
                    error_msg = f"Entry {i}: Found match but couldn't parse JSON: {str(e)[:100]}"
                    error_messages.append(error_msg)
            else:
                # Check if 'data: ' exists at all
                if 'data: ' in text_payload:
                    snippet = text_payload[text_payload.find('data: '):text_payload.find('data: ')+50]
                    error_msg = f"Entry {i}: Found 'data: ' but pattern didn't match. Sample: {snippet}..."
                else:
                    error_msg = f"Entry {i}: No 'data: ' pattern found in textPayload"
                error_messages.append(error_msg)
        
        return successful_results, matching_objects, error_messages, data
            
    except FileNotFoundError:
        print(f"File not found: {json_path}")
        return [], [], [f"File not found: {json_path}"], []
    except json.JSONDecodeError:
        print(f"Invalid JSON in file: {json_path}")
        return [], [], [f"Invalid JSON in file: {json_path}"], []
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return [], [], [f"An error occurred: {str(e)}"], []

def main():
    parser = argparse.ArgumentParser(description='Extract config.data with payload from JSON textPayloads')
    parser.add_argument('json_path', help='Path to the JSON file containing array of objects')
    parser.add_argument('-o', '--output', help='Output file path for extracted data (optional)')
    parser.add_argument('-i', '--index', type=int, help='Extract only the specified index (optional)')
    parser.add_argument('-e', '--errors', action='store_true', help='Show detailed error messages for non-matching entries')
    parser.add_argument('-s', '--sample', type=int, default=3, help='Number of error samples to show (default: 3)')
    parser.add_argument('--save-matches', help='Save the original matching objects to this file (optional)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode with extra logging')
    parser.add_argument('--example', action='store_true', help='Show a random example from the input to help diagnose patterns')
    parser.add_argument('--failed-example', action='store_true', help='Show a random example that failed to match')
    parser.add_argument('--save-failures', help='Save objects that failed to match to this file')
    
    args = parser.parse_args()
    
    # Handle example mode (for diagnosis)
    if (args.example or args.failed_example) and args.json_path:
        try:
            with open(args.json_path, 'r') as file:
                data = json.load(file)
            
            if not isinstance(data, list):
                data = [data]
                
            if len(data) == 0:
                print("No entries found in the file")
                return
                
            import random
            
            if args.failed_example:
                # Process all entries to find failures
                print("Analyzing entries to find failures...")
                failed_entries = []
                
                for i, item in enumerate(data):
                    if 'textPayload' not in item:
                        failed_entries.append((i, item, "No textPayload field"))
                        continue
                        
                    text_payload = item['textPayload']
                    if 'data: ' not in text_payload:
                        failed_entries.append((i, item, "No 'data: ' pattern"))
                        continue
                        
                    # Check if it matches our regex patterns
                    matched = False
                    for pattern in [
                        r"data: '(\{\"payload\":.*?)'(?=\n|\r|$|\s*\})",
                        r'data: "(\{\"payload\":.*?)"(?=\n|\r|$|\s*\})',
                        r"data: '(\{.*?\})'(?=\n|\r|$|\s*\})"
                    ]:
                        if re.search(pattern, text_payload):
                            matched = True
                            break
                            
                    if not matched:
                        failed_entries.append((i, item, "Regex pattern didn't match"))
                
                if not failed_entries:
                    print("No failed entries found! All entries match the patterns.")
                    # Continue with normal processing
                else:
                    # Select a random failure
                    idx, sample, reason = random.choice(failed_entries)
                    print(f"\nRandom FAILED match (Entry #{idx}, Reason: {reason}):")
                    
                    if 'textPayload' in sample:
                        text = sample['textPayload']
                        print("\nTextPayload structure:")
                        
                        # Print the middle section where 'data:' might be
                        if 'data: ' in text:
                            start_idx = max(0, text.find('data: ') - 50)
                            end_idx = min(len(text), text.find('data: ') + 200)
                            
                            before_data = text[start_idx:text.find('data: ')]
                            data_part = text[text.find('data: '):end_idx]
                            
                            print(f"CONTEXT BEFORE 'data:': {before_data}")
                            print(f"DATA SECTION: {data_part}")
                            
                            # Analyze the data format
                            print("\nFormatting analysis:")
                            data_start = text.find('data: ') + 6  # Skip 'data: '
                            
                            if data_start < len(text):
                                # Check what character follows "data: "
                                next_char = text[data_start:data_start+1]
                                print(f"  First character after 'data: ': '{next_char}'")
                                
                                # Check if it contains payload
                                if '"payload"' in text[data_start:data_start+50] or "'payload'" in text[data_start:data_start+50]:
                                    print("  Contains 'payload' within first 50 chars: Yes")
                                else:
                                    print("  Contains 'payload' within first 50 chars: No")
                                    
                                # Check for escaping
                                if '\\\"' in text[data_start:data_start+50]:
                                    print("  Contains escaped quotes (\\\"): Yes")
                                else:
                                    print("  Contains escaped quotes (\\\"): No")
                        else:
                            print("No 'data: ' found in this example")
                            print(f"First 200 chars: {text[:200]}")
                    else:
                        print("No textPayload field found in this example")
                        print(f"Keys available: {list(sample.keys())}")
                    
                    print("\n--- Continuing with normal processing ---\n")
                    # Continue with normal processing, don't return
                
            elif args.example:  # Regular example
                sample_idx = random.randint(0, len(data)-1)
                sample = data[sample_idx]
                print(f"\nRandom example (Entry #{sample_idx}):")
            
                if 'textPayload' in sample:
                    text = sample['textPayload']
                    print("\nTextPayload structure:")
                    
                    # Print the middle section where 'data:' might be
                    if 'data: ' in text:
                        start_idx = max(0, text.find('data: ') - 50)
                        end_idx = min(len(text), text.find('data: ') + 200)
                        
                        before_data = text[start_idx:text.find('data: ')]
                        data_part = text[text.find('data: '):end_idx]
                        
                        print(f"CONTEXT BEFORE 'data:': {before_data}")
                        print(f"DATA SECTION: {data_part}")
                        
                        # Analyze the data format
                        print("\nFormatting analysis:")
                        data_start = text.find('data: ') + 6  # Skip 'data: '
                        
                        if data_start < len(text):
                            # Check what character follows "data: "
                            next_char = text[data_start:data_start+1]
                            print(f"  First character after 'data: ': '{next_char}'")
                            
                            # Check if it contains payload
                            if '"payload"' in text[data_start:data_start+50] or "'payload'" in text[data_start:data_start+50]:
                                print("  Contains 'payload' within first 50 chars: Yes")
                            else:
                                print("  Contains 'payload' within first 50 chars: No")
                                
                            # Check for escaping
                            if '\\\"' in text[data_start:data_start+50]:
                                print("  Contains escaped quotes (\\\"): Yes")
                            else:
                                print("  Contains escaped quotes (\\\"): No")
                    else:
                        print("No 'data: ' found in this example")
                        print(f"First 200 chars: {text[:200]}")
                else:
                    print("No textPayload field found in this example")
                    print(f"Keys available: {list(sample.keys())}")
                
                print("\n--- Continuing with normal processing ---\n")
                # Continue with normal processing, don't return
                
        except Exception as e:
            print(f"Error examining example: {str(e)}")
            print("\n--- Continuing with normal processing ---\n")
            # Continue with normal processing, don't return
    
    config_data_list, matching_objects, error_messages, all_data = extract_config_data(args.json_path, args.debug)
    
    # Print summary statistics
    total_entries = len(matching_objects) + len(error_messages)
    print(f"Summary:")
    print(f"  Total entries processed: {total_entries}")
    print(f"  Successful matches: {len(matching_objects)} ({len(matching_objects)/total_entries*100:.1f}%)")
    print(f"  Failed matches: {len(error_messages)} ({len(error_messages)/total_entries*100:.1f}%)")
    
    # Save failed entries if requested
    if args.save_failures and len(error_messages) > 0:
        # Collect the original objects that failed to match
        failed_objects = []
        for i, item in enumerate(data):
            if item not in matching_objects:
                failed_objects.append(item)
                
        with open(args.save_failures, 'w') as file:
            json.dump(failed_objects, file, indent=2)
        print(f"\nSaved {len(failed_objects)} failed matches to {args.save_failures}")
    
    if not config_data_list:
        print("\nNo matching config.data values found.")
        
        # Show error samples if requested
        if args.errors and error_messages:
            sample_size = min(args.sample, len(error_messages))
            print(f"\nShowing {sample_size} error samples:")
            for i in range(sample_size):
                print(f"  {error_messages[i]}")
                
        # Provide additional hints
        print("\nTroubleshooting suggestions:")
        print("  1. Run with --failed-example to see a random example that didn't match")
        print("  2. Run with --example flag to see a random sample of your data")
        print("  3. Run with --debug flag to see more detailed processing information")
        print("  4. Use --save-failures to save all objects that didn't match")
        return
    
    # Handle specific index if provided
    if args.index is not None:
        if 0 <= args.index < len(config_data_list):
            config_data_list = [config_data_list[args.index]]
            matching_objects = [matching_objects[args.index]]
        else:
            print(f"Index {args.index} is out of range. Valid range: 0-{len(config_data_list)-1}")
            return
    
    # Format the output - either a list or single item
    if len(config_data_list) == 1:
        output_data = config_data_list[0]
    else:
        output_data = config_data_list
    
    formatted_output = json.dumps(output_data, indent=2)
    
    # Save or print the extracted data
    if args.output:
        with open(args.output, 'w') as file:
            file.write(formatted_output)
        print(f"\nExtracted config.data saved to {args.output}")
    else:
        print("\nExtracted config.data preview (first item if multiple):")
        # Show just a sample of the output if it's very large
        preview = json.dumps(config_data_list[0], indent=2)
        if len(preview) > 500:
            print(preview[:500] + "...\n(output truncated)")
        else:
            print(preview)
    
    # Save matching original objects if requested
    if args.save_matches:
        if len(matching_objects) == 1:
            matches_output = matching_objects[0]
        else:
            matches_output = matching_objects
            
        with open(args.save_matches, 'w') as file:
            json.dump(matches_output, file, indent=2)
        print(f"Original matching objects saved to {args.save_matches}")

    # Show error samples if requested
    if args.errors and error_messages:
        sample_size = min(args.sample, len(error_messages))
        print(f"\nShowing {sample_size} error samples:")
        for i in range(sample_size):
            print(f"  {error_messages[i]}")

if __name__ == "__main__":
    main()