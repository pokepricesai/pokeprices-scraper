"""
PokePrices Card Matcher v1
===========================
Matches eBay listing titles to the correct card variant in our Supabase database.

This is the core matching logic that prevents false deals. Given an eBay title like:
  "Pokemon Pikachu Gold Star 104/115 Holon Phantoms PSA 10 GEM MINT"

It finds the correct card in our database:
  card_slug: 889184, card_name: "Pikachu [Gold Star] #104", set: "Holon Phantoms"

And determines the right price to compare against:
  PSA 10 price: $23,500 (not the raw price of $2,122)

The matching works by:
  1. Parsing the eBay title into structured components
  2. Loading all candidate cards that could match (same base name + set)
  3. Scoring each candidate based on variant/number match
  4. Returning the best match with the correct fair value for the grade

Can be tested standalone:
  python card_matcher.py --test
"""

import re


# ============================================
# VARIANT DEFINITIONS
# ============================================

# These variants define fundamentally different cards with different values.
# If a card has [Gold Star] and the eBay title doesn't mention Gold Star,
# it's NOT a match. And vice versa — if the title says Gold Star but our
# card doesn't have that tag, it's also NOT a match.
VALUE_VARIANTS = {
    "1st edition":    ["1st edition", "1st ed", "1st ed.", "first edition"],
    "gold star":      ["gold star", "goldstar", "gold ★", " ★ "],
    "shadowless":     ["shadowless", "shadow less"],
    "1999-2000":      ["1999-2000", "1999 2000", "uk print", "4th print"],
    "black dot error": ["black dot error", "black dot"],
    "tekno":          ["tekno", "techno"],
    "sparkle":        ["sparkle", "spectra"],
    "for position only": ["for position only", "fpo", "test print"],
    "trainer deck a": ["trainer deck a"],
    "trainer deck b": ["trainer deck b"],
    "prerelease staff": ["prerelease staff", "pre-release staff", "staff prerelease"],
    "prerelease":     ["prerelease", "pre-release", "pre release"],
    "staff":          ["staff"],
    "error":          ["error", "misprint"],
    "double holo error": ["double holo", "double holo error"],
    "stadium challenge": ["stadium challenge"],
    "cosmos professor program": ["cosmos professor program", "cosmos professor"],
    "professor program": ["professor program", "professor promo"],
    "championships":  ["championship", "championships", "worlds"],
    "quarter-finalist": ["quarter-finalist", "quarter finalist", "qf"],
    "semi-finalist":  ["semi-finalist", "semi finalist", "sf"],
    "top thirty-two": ["top thirty-two", "top 32", "top thirty two"],
    "top 8":          ["top 8", "top eight"],
    "ultra ball league": ["ultra ball league"],
    "premier ball league": ["premier ball league"],
    "regional championship staff": ["regional championship staff"],
}

# Cosmetic variants — they differentiate cards but in a more standard way
COSMETIC_VARIANTS = {
    "reverse holo":   ["reverse holo", "reverse holographic", "rev holo", "reverse"],
    "holo":           ["holo", "holographic", "holofoil"],
    "foil":           ["foil"],
    "rainbow foil":   ["rainbow foil", "rainbow"],
    "non-holo":       ["non-holo", "non holo", "nonholo"],
}


# ============================================
# TITLE PARSING
# ============================================

def parse_ebay_title(title):
    """Parse an eBay listing title into structured components.
    
    Returns:
        dict with keys:
          - title_lower: lowercase full title
          - base_words: significant words (Pokemon names etc.)
          - card_number: extracted card number (e.g. '4', '104', 'XY176')
          - grading_company: 'PSA', 'CGC', 'BGS', etc. or None
          - grade_number: '10', '9', '9.5' etc. or None
          - is_graded: True/False
          - value_variants_found: list of value variants detected in title
          - cosmetic_variants_found: list of cosmetic variants detected in title
    """
    if not title:
        return None
    
    title_lower = title.lower()
    
    # Extract grading info
    grading_company = None
    grade_number = None
    is_graded = False
    
    grade_match = re.search(
        r'\b(PSA|CGC|BGS|SGC|ACE|AGS|TAG|GMA|MNT)\s*(\d+\.?\d*)\b',
        title, re.IGNORECASE
    )
    if grade_match:
        grading_company = grade_match.group(1).upper()
        grade_number = grade_match.group(2)
        is_graded = True
    elif re.search(r'\bgraded\b', title_lower):
        is_graded = True
    
    # Extract card number — look for patterns like:
    #   "4/102", "#4", "No. 4", "004/102", "104/115"
    card_number = None
    
    # Pattern: number/total (most common on eBay)
    num_match = re.search(r'(\d{1,4})\s*/\s*\d{1,4}', title)
    if num_match:
        card_number = num_match.group(1).lstrip("0") or "0"
    
    # Pattern: #number
    if not card_number:
        num_match = re.search(r'#\s*([A-Za-z]*\d+[A-Za-z]*)', title)
        if num_match:
            card_number = num_match.group(1)
    
    # Detect value variants in title
    value_variants_found = []
    for variant_key, aliases in VALUE_VARIANTS.items():
        for alias in aliases:
            if alias.lower() in title_lower:
                value_variants_found.append(variant_key)
                break
    
    # Detect cosmetic variants in title
    cosmetic_variants_found = []
    for variant_key, aliases in COSMETIC_VARIANTS.items():
        for alias in aliases:
            if alias.lower() in title_lower:
                cosmetic_variants_found.append(variant_key)
                break
    
    # Handle "holo" detection more carefully
    # "reverse holo" should not also trigger "holo"
    if "reverse holo" in cosmetic_variants_found and "holo" in cosmetic_variants_found:
        cosmetic_variants_found.remove("holo")
    
    return {
        "title_lower": title_lower,
        "card_number": card_number,
        "grading_company": grading_company,
        "grade_number": grade_number,
        "is_graded": is_graded,
        "value_variants_found": value_variants_found,
        "cosmetic_variants_found": cosmetic_variants_found,
    }


def parse_card_name(card_name):
    """Parse a PriceCharting card name into structured components.
    
    e.g. 'Charizard [1st Edition] #4' → {
        base_name: 'Charizard',
        card_number: '4',
        value_variants: ['1st edition'],
        cosmetic_variants: [],
    }
    """
    if not card_name:
        return None
    
    # Extract bracket tags
    all_tags = re.findall(r'\[(.*?)\]', card_name)
    
    value_vars = []
    cosmetic_vars = []
    
    for tag in all_tags:
        tag_lower = tag.lower()
        matched = False
        
        # Check value variants
        for variant_key in VALUE_VARIANTS:
            if variant_key in tag_lower:
                value_vars.append(variant_key)
                matched = True
                break
        
        if not matched:
            # Check cosmetic variants
            for variant_key in COSMETIC_VARIANTS:
                if variant_key in tag_lower:
                    cosmetic_vars.append(variant_key)
                    matched = True
                    break
    
    # Extract base name (strip brackets and number)
    clean = re.sub(r'\[.*?\]', '', card_name).strip()
    num_match = re.search(r'#(\S+)', clean)
    card_number = num_match.group(1) if num_match else None
    base_name = re.sub(r'#\S+', '', clean).strip()
    
    return {
        "base_name": base_name,
        "card_number": card_number,
        "value_variants": value_vars,
        "cosmetic_variants": cosmetic_vars,
    }


def parse_card_identity(card_name, set_name):
    """Parse card name and build an eBay search query.
    
    Used by the scraper to construct the initial search.
    Returns dict with search_query and parsed identity.
    """
    if not card_name:
        return None
    
    parsed = parse_card_name(card_name)
    if not parsed:
        return None
    
    # Build search query including important variants
    clean_set = set_name or ""
    clean_set = re.sub(r'^Pokemon\s+', '', clean_set, flags=re.IGNORECASE).strip()
    
    parts = ["Pokemon", parsed["base_name"]]
    
    # Add value variants to search query (Gold Star, 1st Edition, etc.)
    for var in parsed["value_variants"]:
        # Use the original bracket text for better search results
        tags = re.findall(r'\[(.*?)\]', card_name)
        for tag in tags:
            if var in tag.lower():
                parts.append(tag)
                break
    
    if clean_set:
        parts.append(clean_set)
    if parsed["card_number"]:
        parts.append(parsed["card_number"])
    
    search_query = re.sub(r'\s+', ' ', " ".join(parts)).strip()
    
    return {
        "base_name": parsed["base_name"],
        "card_number": parsed["card_number"],
        "value_variants": parsed["value_variants"],
        "cosmetic_variants": parsed["cosmetic_variants"],
        "set_name": clean_set,
        "search_query": search_query,
    }


# ============================================
# MATCHING LOGIC
# ============================================

def score_match(ebay_parsed, card_parsed):
    """Score how well an eBay listing matches a specific card variant.
    
    Returns (score, breakdown) where:
      score > 0 = plausible match
      score ≤ 0 = not a match
      
    Scoring:
      +20  base name found in title
      +15  card number matches
      +10  per value variant that matches (both have it)
      -50  per value variant MISMATCH (card has it, title doesn't OR title has it, card doesn't)
      +3   per cosmetic variant that matches
      -10  per cosmetic variant mismatch
    """
    if not ebay_parsed or not card_parsed:
        return -100, ["no data"]
    
    score = 0
    breakdown = []
    
    # 1. Base name check
    base_lower = card_parsed["base_name"].lower()
    base_first_word = base_lower.split()[0] if base_lower else ""
    
    if base_first_word and base_first_word in ebay_parsed["title_lower"]:
        score += 20
        breakdown.append(f"+20 name '{base_first_word}'")
    else:
        score -= 100
        breakdown.append(f"-100 name '{base_first_word}' NOT FOUND")
        return score, breakdown  # No point continuing
    
    # 2. Card number check
    if card_parsed["card_number"] and ebay_parsed["card_number"]:
        card_num_clean = card_parsed["card_number"].lstrip("0") or "0"
        ebay_num_clean = ebay_parsed["card_number"].lstrip("0") or "0"
        
        # Also handle alphanumeric numbers like "XY176", "SWSH241"
        if card_num_clean.lower() == ebay_num_clean.lower():
            score += 15
            breakdown.append(f"+15 number '{card_num_clean}'")
        else:
            score -= 20
            breakdown.append(f"-20 number mismatch: card='{card_num_clean}' ebay='{ebay_num_clean}'")
    
    # 3. Value variant matching (THE CRITICAL PART)
    card_value_vars = set(card_parsed["value_variants"])
    ebay_value_vars = set(ebay_parsed["value_variants_found"])
    
    # Variants in both (good — confirmed match)
    shared_value = card_value_vars & ebay_value_vars
    for v in shared_value:
        score += 10
        breakdown.append(f"+10 value variant '{v}' MATCH")
    
    # Variants in card but NOT in title (bad — probably wrong version)
    card_only = card_value_vars - ebay_value_vars
    for v in card_only:
        score -= 50
        breakdown.append(f"-50 card has '{v}' but eBay title MISSING it")
    
    # Variants in title but NOT in card (bad — eBay listing is a different variant)
    ebay_only = ebay_value_vars - card_value_vars
    for v in ebay_only:
        # Special case: if the eBay title says "1st edition" but our card doesn't have it,
        # that's a strong negative signal — it's a different, likely MORE valuable card
        score -= 50
        breakdown.append(f"-50 eBay title has '{v}' but card DOESN'T")
    
    # 4. Cosmetic variant matching (less critical but still matters)
    card_cos_vars = set(card_parsed["cosmetic_variants"])
    ebay_cos_vars = set(ebay_parsed["cosmetic_variants_found"])
    
    shared_cos = card_cos_vars & ebay_cos_vars
    for v in shared_cos:
        score += 3
        breakdown.append(f"+3 cosmetic '{v}' match")
    
    card_cos_only = card_cos_vars - ebay_cos_vars
    for v in card_cos_only:
        score -= 10
        breakdown.append(f"-10 card has cosmetic '{v}' but title missing")
    
    ebay_cos_only = ebay_cos_vars - card_cos_vars
    for v in ebay_cos_only:
        score -= 10
        breakdown.append(f"-10 title has cosmetic '{v}' but card doesn't")
    
    return score, breakdown


def find_best_match(ebay_title, candidates):
    """Find the best matching card from a list of candidates.
    
    Args:
        ebay_title: the eBay listing title string
        candidates: list of card dicts from card_trends, each with
                   card_slug, card_name, set_name, current_raw, current_psa10, current_psa9
    
    Returns:
        (best_card, score, breakdown, confidence) or (None, 0, [], 'none')
    """
    ebay_parsed = parse_ebay_title(ebay_title)
    if not ebay_parsed:
        return None, 0, [], "none"
    
    best_card = None
    best_score = -999
    best_breakdown = []
    
    for card in candidates:
        card_parsed = parse_card_name(card.get("card_name", ""))
        if not card_parsed:
            continue
        
        score, breakdown = score_match(ebay_parsed, card_parsed)
        
        if score > best_score:
            best_score = score
            best_card = card
            best_breakdown = breakdown
    
    # Convert score to confidence level
    if best_score >= 35:
        confidence = "high"      # Name + number + all variants match
    elif best_score >= 20:
        confidence = "medium"    # Name matches, most things line up  
    elif best_score >= 0:
        confidence = "low"       # Marginal match, likely issues
    else:
        confidence = "none"      # Not a match
    
    return best_card, best_score, best_breakdown, confidence


def get_fair_value(card, ebay_parsed):
    """Get the correct fair value based on grading.
    
    If eBay listing is PSA 10, use PSA 10 price.
    If PSA 9, use PSA 9 price.
    If ungraded, use raw price.
    """
    if not ebay_parsed or not card:
        return 0, "Unknown"
    
    if ebay_parsed["is_graded"]:
        grade = ebay_parsed.get("grade_number", "")
        company = ebay_parsed.get("grading_company", "")
        
        if grade == "10":
            val = card.get("current_psa10")
            if val and val > 0:
                return val, f"{company} {grade}"
        
        if grade and float(grade) >= 9:
            val = card.get("current_psa9")
            if val and val > 0:
                return val, f"{company} {grade}"
        
        # Graded but lower grade or unknown — use PSA 9 as estimate
        val = card.get("current_psa9")
        if val and val > 0:
            return val, f"Graded (est)"
    
    # Ungraded
    raw = card.get("current_raw", 0)
    return raw, "Raw"


# ============================================
# TESTING
# ============================================

def run_tests():
    """Test the matching logic with known examples."""
    
    # Simulated candidate cards (what's in our database)
    charizard_candidates = [
        {"card_slug": "715593", "card_name": "Charizard [1st Edition] #4", "set_name": "Base Set",
         "current_raw": 544471, "current_psa10": 16818352, "current_psa9": 5000000},
        {"card_slug": "715695", "card_name": "Charizard [Shadowless] #4", "set_name": "Base Set",
         "current_raw": 87500, "current_psa10": 3010000, "current_psa9": 800000},
        {"card_slug": "7307451", "card_name": "Charizard [Black Dot Error] #4", "set_name": "Base Set",
         "current_raw": 47500, "current_psa10": None, "current_psa9": None},
        {"card_slug": "630417", "card_name": "Charizard #4", "set_name": "Base Set",
         "current_raw": 33617, "current_psa10": 1621607, "current_psa9": 500000},
        {"card_slug": "7096109", "card_name": "Charizard [1999-2000] #4", "set_name": "Base Set",
         "current_raw": 32151, "current_psa10": 1244922, "current_psa9": 400000},
    ]
    
    pikachu_candidates = [
        {"card_slug": "889184", "card_name": "Pikachu [Gold Star] #104", "set_name": "Holon Phantoms",
         "current_raw": 212291, "current_psa10": 2350000, "current_psa9": 1000000},
        {"card_slug": "889268", "card_name": "Pikachu [Reverse Holo] #79", "set_name": "Holon Phantoms",
         "current_raw": 8469, "current_psa10": 127273, "current_psa9": 50000},
        {"card_slug": "889159", "card_name": "Pikachu #79", "set_name": "Holon Phantoms",
         "current_raw": 551, "current_psa10": 16250, "current_psa9": 5000},
    ]
    
    test_cases = [
        # (title, candidates, expected_slug, description)
        (
            "Pokemon Charizard 4/102 Base Set 1st Edition PSA 10 GEM MINT",
            charizard_candidates,
            "715593",
            "1st Edition PSA 10 Charizard"
        ),
        (
            "Charizard Holo 4/102 Base Set Unlimited Pokemon Card",
            charizard_candidates,
            "630417",
            "Plain unlimited Charizard"
        ),
        (
            "Pokemon Charizard 4/102 Base Set Shadowless PSA 9 MINT",
            charizard_candidates,
            "715695",
            "Shadowless PSA 9 Charizard"
        ),
        (
            "Charizard Base Set 4/102 1999-2000 4th Print UK",
            charizard_candidates,
            "7096109",
            "1999-2000 print Charizard"
        ),
        (
            "Pokemon Pikachu Gold Star 104/115 Holon Phantoms Near Mint",
            pikachu_candidates,
            "889184",
            "Pikachu Gold Star raw"
        ),
        (
            "Pikachu Reverse Holo 79/115 Holon Phantoms Pokemon",
            pikachu_candidates,
            "889268",
            "Pikachu Reverse Holo"
        ),
        (
            "Pikachu 79/115 Holon Phantoms Pokemon Card",
            pikachu_candidates,
            "889159",
            "Plain Pikachu (no variant)"
        ),
        (
            "Pikachu Holon Phantoms PSA 10 79/115",
            pikachu_candidates,
            "889159",
            "Plain Pikachu PSA 10 (should match plain, not Gold Star)"
        ),
    ]
    
    print("=" * 70)
    print("CARD MATCHER TEST SUITE")
    print("=" * 70 + "\n")
    
    passed = 0
    failed = 0
    
    for title, candidates, expected_slug, description in test_cases:
        best_card, score, breakdown, confidence = find_best_match(title, candidates)
        matched_slug = best_card["card_slug"] if best_card else "NONE"
        
        # Get fair value
        ebay_parsed = parse_ebay_title(title)
        fair_value, value_type = get_fair_value(best_card, ebay_parsed) if best_card else (0, "N/A")
        
        status = "✓ PASS" if matched_slug == expected_slug else "✗ FAIL"
        if matched_slug == expected_slug:
            passed += 1
        else:
            failed += 1
        
        print(f"{status}: {description}")
        print(f"  Title:    {title}")
        print(f"  Matched:  {best_card['card_name'] if best_card else 'NONE'} (slug: {matched_slug})")
        print(f"  Expected: slug {expected_slug}")
        print(f"  Score: {score} | Confidence: {confidence} | Fair value: ${fair_value/100:.2f} ({value_type})")
        print(f"  Breakdown: {breakdown}")
        print()
    
    print(f"{'=' * 70}")
    print(f"Results: {passed} passed, {failed} failed out of {len(test_cases)} tests")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    run_tests()
