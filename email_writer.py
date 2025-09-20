import json
import openai
from datetime import datetime
import boto3

def load_config(path='config.json'):
    with open(path) as f:
        return json.load(f)

def load_digest(path):
    with open(path) as f:
        return json.load(f)

def generate_newsletter(entries, cfg):
    openai.api_key = cfg['gpt_api_key']
    model_name = cfg.get('openai_model', 'gpt-4.1-mini')

    # load prompt template
    prompt_file = cfg.get('prompt_file', 'prompt.html')
    with open(prompt_file, encoding='utf-8') as pf:
        prompt = pf.read().strip() + "\n\n"

    # append each article, gracefully handling missing keys
    for e in entries:
        title = e.get('title', '')
        url = e.get('url', '')
        summary = e.get('summary', '')
        prompt += (
            f"- Title: {title}\n"
            f"  URL: {url}\n"
            f"  Summary: {summary}\n\n"
        )

    prompt += (
        "Organize by topic if possible. "
        "Include a brief disclaimer and a call-to-action at the end."
    )

    resp = openai.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}],
        temperature=1.0,
        max_completion_tokens=10000
    )
    return resp.choices[0].message.content.strip()

def upload_to_s3(html_content: str, bucket: str, key: str):
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=html_content.encode('utf-8'),
        ContentType='text/html'
    )
    print(f"Uploaded newsletter to s3://{bucket}/{key}")

def main():
    cfg = load_config()
    digest = load_digest(cfg.get('output_path', 'digest.json'))
    if not digest:
        print("No entries found in", cfg.get('output_path'))
        return

    # generate content
    newsletter = generate_newsletter(digest, cfg)
    fname = cfg.get('email_output', 'newsletter.html')

    # write local copy (optional)
    with open(fname, 'w', encoding='utf-8') as f:
        f.write(f"Date: {datetime.utcnow().date()}\n\n")
        f.write(newsletter)
    print(f"Wrote newsletter to {fname}")

    # upload newsletter to S3
    bucket = cfg['s3_bucket']
    key_prefix = cfg.get('s3_key_prefix', 'newsletters')
    key = f"{key_prefix}/{fname}"
    with open(fname, 'r', encoding='utf-8') as f:
        html_body = f.read()
    upload_to_s3(html_body, bucket, key)

    print("Generation and upload complete; exiting.")

if __name__ == '__main__':
    main()
