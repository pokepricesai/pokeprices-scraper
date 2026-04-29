#!/usr/bin/env python3
"""
One-off script: fetch images + pc_urls for Ascended Heroes from PriceCharting.
Run locally: python fix_ascended_heroes_images.py
Requires: pip install requests beautifulsoup4 supabase python-dotenv
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_KEY']
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

PC_SET_SLUG = 'pokemon-ascended-heroes'
PC_BASE = 'https://www.pricecharting.com'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

def card_name_to_pc_slug(card_name: str) -> str:
    """Convert card name to PriceCharting URL slug format."""
    # Remove brackets content but keep the rest
    # "Pikachu ex #277" -> "pikachu-ex-277"
    # "Acerola's Mischief [Ball] #180" -> "acerolas-mischief-ball-180"
    slug = card_name.lower()
    slug = slug.replace("'s", 's').replace("'", '')
    slug = re.sub(r'[^\w\s#\[\]]', '', slug)
    slug = slug.replace('[', '').replace(']', '')
    slug = slug.replace('#', '')
    slug = re.sub(r'\s+', '-', slug.strip())
    slug = re.sub(r'-+', '-', slug)
    return slug

def fetch_image_from_pc(pc_slug: str) -> tuple[str | None, str | None]:
    """
    Fetch a card page from PriceCharting and extract the image URL.
    Returns (image_url, pc_url) or (None, None) if not found.
    """
    url = f'{PC_BASE}/game/{PC_SET_SLUG}/{pc_slug}'
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 404:
            return None, None
        if resp.status_code != 200:
            print(f'  HTTP {resp.status_code} for {url}')
            return None, None

        soup = BeautifulSoup(resp.text, 'html.parser')

        # PriceCharting card image is in #product_image or similar
        img = (
            soup.select_one('#product_image img') or
            soup.select_one('.product-image img') or
            soup.select_one('img#photo') or
            soup.select_one('img[itemprop="image"]')
        )

        if img:
            src = img.get('src') or img.get('data-src')
            if src:
                if src.startswith('//'):
                    src = 'https:' + src
                elif src.startswith('/'):
                    src = PC_BASE + src
                return src, url

        return None, url  # page found but no image yet

    except Exception as e:
        print(f'  Error fetching {url}: {e}')
        return None, None


def main():
    # Fetch all cards in the set that are missing images
    print('Fetching Ascended Heroes cards missing images...')
    result = supabase.from_('cards') \
        .select('id, card_name, card_slug, image_url, pc_url') \
        .eq('set_name', 'Ascended Heroes') \
        .is_('image_url', 'null') \
        .execute()

    cards = result.data
    print(f'Found {len(cards)} cards missing images\n')

    updated = 0
    no_image = 0
    errors = 0

    for i, card in enumerate(cards):
        card_name = card['card_name']
        card_id = card['id']

        pc_slug = card_name_to_pc_slug(card_name)
        print(f'[{i+1}/{len(cards)}] {card_name} -> {pc_slug}')

        image_url, pc_url = fetch_image_from_pc(pc_slug)

        if image_url:
            supabase.from_('cards').update({
                'image_url': image_url,
                'pc_url': pc_url,
            }).eq('id', card_id).execute()
            print(f'  ✅ Updated: {image_url[:60]}...')
            updated += 1
        elif pc_url:
            # Page exists, just no image yet — still save the pc_url
            supabase.from_('cards').update({
                'pc_url': pc_url,
            }).eq('id', card_id).execute()
            print(f'  ⚠️  Page found but no image yet — pc_url saved')
            no_image += 1
        else:
            print(f'  ❌ Not found on PriceCharting')
            errors += 1

        # Be polite — 0.5s between requests
        time.sleep(0.5)

    print(f'\nDone!')
    print(f'  Updated with images: {updated}')
    print(f'  Page found, no image yet: {no_image}')
    print(f'  Not found: {errors}')

    if no_image > 0:
        print(f'\nNote: {no_image} cards have pc_urls but no images yet.')
        print('PriceCharting sometimes adds images a few days after set release.')
        print('Re-run this script in a day or two and images should populate.')


if __name__ == '__main__':
    main()
