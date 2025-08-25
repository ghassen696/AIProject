from uti.Classification_cat import classification_categories2

def build_prompt(employee_logs):
    # Limit each log to only essential text (truncate long input text)
    joined_text = "\n".join([
        f"[{log['timestamp']}] ({log['application']}) {log.get('window', '')[:40]} - {log.get('text', '')[:80]}"
        for log in employee_logs
    ])

    # Use compact list of categories (no dashes or extra formatting)
    category_list = ", ".join(classification_categories2)

    # Return shorter, clean instruction prompt
    return f"""You are a productivity assistant.
List of logs:
{joined_text}
From the logs:
- Determine what technical tasks the employee worked on.
- Choose 1 or more categories from: {category_list}
Do NOT include explanation, markdown, or code blocks.Respond ONLY with a JSON object like:
{{"categories": [...], "summary": "One short paragraph, no newlines"}}"""
