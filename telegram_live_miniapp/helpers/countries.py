"""v9.76: справочники стран/лиг и логика сортировки.

Чистые функции без зависимости от глобалов сервера. Вынесены из app.py,
чтобы:
  - не пересобирать regex-таблицы при каждом обращении;
  - можно было покрыть unit-тестами;
  - дальнейший рефакторинг (storage.py / notifier.py) опирался на стабильное API.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any


RUS_COUNTRY_TO_CODE = {
    "англия": "GB",
    "испания": "ES",
    "италия": "IT",
    "германия": "DE",
    "франция": "FR",
    "нидерланды": "NL",
    "португалия": "PT",
    "турция": "TR",
    "бразилия": "BR",
    "аргентина": "AR",
    "сша": "US",
}

COUNTRY_CODE_MAP = {
    "england": "GB", "scotland": "GB", "wales": "GB", "northern ireland": "GB", "great britain": "GB", "uk": "GB",
    "spain": "ES", "italy": "IT", "germany": "DE", "france": "FR", "netherlands": "NL", "portugal": "PT", "turkey": "TR",
    "brazil": "BR", "argentina": "AR", "arg": "AR", "usa": "US", "united states": "US", "russia": "RU",
    "barbados": "BB", "dominican republic": "DO", "dominicana": "DO", "colombia": "CO",
    "bhutan": "BT", "egypt": "EG", "ethiopia": "ET", "kenya": "KE", "paraguay": "PY", "china": "CN", "india": "IN", "north korea": "KP", "south korea": "KR",
    "thailand": "TH", "japan": "JP", "australia": "AU", "new zealand": "NZ", "albania": "AL", "algeria": "DZ",
    "angola": "AO", "armenia": "AM", "austria": "AT", "azerbaijan": "AZ", "bahrain": "BH", "belarus": "BY",
    "belgium": "BE", "bolivia": "BO", "bosnia": "BA", "bosnia and herzegovina": "BA", "bulgaria": "BG",
    "cameroon": "CM", "canada": "CA", "chile": "CL", "costa rica": "CR", "croatia": "HR",
    "cyprus": "CY", "czech republic": "CZ", "czechia": "CZ", "denmark": "DK", "ecuador": "EC",
    "estonia": "EE", "finland": "FI", "georgia": "GE", "ghana": "GH", "greece": "GR", "guatemala": "GT",
    "honduras": "HN", "hong kong": "HK", "hungary": "HU", "iceland": "IS", "indonesia": "ID", "iran": "IR",
    "iraq": "IQ", "ireland": "IE", "israel": "IL", "jordan": "JO", "kazakhstan": "KZ", "kosovo": "XK",
    "kuwait": "KW", "latvia": "LV", "lebanon": "LB", "lithuania": "LT", "luxembourg": "LU", "malaysia": "MY",
    "malta": "MT", "mexico": "MX", "moldova": "MD", "montenegro": "ME", "morocco": "MA", "nigeria": "NG",
    "norway": "NO", "oman": "OM", "panama": "PA", "peru": "PE", "poland": "PL", "qatar": "QA",
    "romania": "RO", "saudi arabia": "SA", "serbia": "RS", "singapore": "SG", "slovakia": "SK", "slovenia": "SI",
    "south africa": "ZA", "sweden": "SE", "switzerland": "CH", "syria": "SY", "tunisia": "TN", "ukraine": "UA",
    "uruguay": "UY", "uzbekistan": "UZ", "venezuela": "VE", "vietnam": "VN", "zambia": "ZM", "zimbabwe": "ZW",
    "yemen": "YE",
    "tanzania": "TZ", "uganda": "UG", "congo": "CG", "republic of the congo": "CG",
    "dr congo": "CD", "drc": "CD", "democratic republic of the congo": "CD",
    "democratic republic congo": "CD", "congo dr": "CD", "congo kinshasa": "CD",
    "liberia": "LR", "sierra leone": "SL", "gambia": "GM", "guinea": "GN", "guinea-bissau": "GW",
    "burkina faso": "BF", "malawi": "MW", "eswatini": "SZ", "swaziland": "SZ", "lesotho": "LS",
    "libya": "LY", "mauritania": "MR", "niger": "NE", "togo": "TG", "benin": "BJ",
    "burundi": "BI", "central african republic": "CF", "chad": "TD", "equatorial guinea": "GQ",
    "gabon": "GA", "madagascar": "MG", "mauritius": "MU", "seychelles": "SC",
    "afghanistan": "AF", "bangladesh": "BD", "nepal": "NP", "pakistan": "PK", "maldives": "MV",
    "sri lanka": "LK", "myanmar": "MM", "laos": "LA", "cambodia": "KH", "brunei": "BN", "mongolia": "MN",
    "turkmenistan": "TM", "kyrgyzstan": "KG", "tajikistan": "TJ",
    "ivory coast": "CI", "senegal": "SN", "mali": "ML", "rwanda": "RW",
    "mozambique": "MZ", "namibia": "NA", "botswana": "BW",
    "somalia": "SO", "eritrea": "ER", "sudan": "SD", "djibouti": "DJ",
    "united arab emirates": "AE", "uae": "AE",
    "andorra": "AD", "fiji": "FJ", "curacao": "CW",
}


CONTINENT_NAMES = {
    "africa", "asia", "americas", "america", "europe", "oceania", "international", "world",
    "без страны", "без лиги",
}


CONTINENT_FLAG_EMOJI = {
    "europe": "🇪🇺",
    "africa": "🌍",
    "americas": "🌎", "america": "🌎",
    "north america": "🌎", "south america": "🌎", "central america": "🌎",
    "asia": "🌏",
    "oceania": "🌏",
    "international": "🏳️", "world": "🌐",
}


COUNTRY_NAME_ALIASES = {
    "bhutan": "Bhutan", "bhutanese": "Bhutan", "egyptian": "Egypt", "ethiopian": "Ethiopia", "kenyan": "Kenya", "paraguayan": "Paraguay", "chinese": "China", "indian": "India",
    "hku": "Hong Kong", "korean": "South Korea", "north korean": "North Korea", "thai": "Thailand", "japanese": "Japan",
    "australian": "Australia", "albanian": "Albania", "brazilian": "Brazil", "argentine": "Argentina", "argentinian": "Argentina",
    "english": "England", "spanish": "Spain", "italian": "Italy", "german": "Germany", "french": "France",
    "dutch": "Netherlands", "portuguese": "Portugal", "turkish": "Turkey", "russian": "Russia",
    "polish": "Poland", "romanian": "Romania", "serbian": "Serbia", "swedish": "Sweden", "norwegian": "Norway",
    "indonesian": "Indonesia", "indonesia": "Indonesia",
    "malaysian": "Malaysia", "vietnamese": "Vietnam", "filipino": "Philippines", "philippines": "Philippines",
    "mexican": "Mexico", "colombian": "Colombia", "chilean": "Chile", "peruvian": "Peru",
    "ecuadorian": "Ecuador", "venezuelan": "Venezuela", "uruguayan": "Uruguay", "bolivian": "Bolivia",
    "costarica": "Costa Rica", "costaricean": "Costa Rica", "costarican": "Costa Rica",
    "honduran": "Honduras", "guatemalan": "Guatemala", "panamanian": "Panama",
    "saudi": "Saudi Arabia", "emirati": "United Arab Emirates", "qatari": "Qatar", "iranian": "Iran",
    "iraqi": "Iraq", "lebanese": "Lebanon", "syrian": "Syria", "moroccan": "Morocco", "tunisian": "Tunisia",
    "algerian": "Algeria", "nigerian": "Nigeria", "ghanaian": "Ghana", "ugandan": "Uganda",
    "tanzanian": "Tanzania", "tanzania": "Tanzania", "tanzanian premier": "Tanzania",
    "uganda": "Uganda", "uganda premier": "Uganda",
    "dr congo": "DR Congo", "drc": "DR Congo", "congo dr": "DR Congo",
    "democratic republic of the congo": "DR Congo", "democratic republic congo": "DR Congo",
    "congo kinshasa": "DR Congo", "vodacom ligue": "DR Congo",
    "congolese": "Congo", "congo": "Congo",
    "sudanese": "Sudan", "zambian": "Zambia", "zimbabwean": "Zimbabwe",
    "kazakhstani": "Kazakhstan", "uzbek": "Uzbekistan", "azerbaijani": "Azerbaijan", "georgian": "Georgia",
    "armenian": "Armenia", "belarusian": "Belarus", "ukrainian": "Ukraine", "moldovan": "Moldova",
    "estonian": "Estonia", "latvian": "Latvia", "lithuanian": "Lithuania", "finnish": "Finland",
    "danish": "Denmark", "icelandic": "Iceland", "irish": "Ireland", "scottish": "Scotland", "welsh": "Wales",
    "czech": "Czech Republic", "slovak": "Slovakia", "slovenian": "Slovenia", "croatian": "Croatia",
    "bosnian": "Bosnia and Herzegovina", "macedonian": "North Macedonia", "north macedonian": "North Macedonia",
    "north macedonia": "North Macedonia", "kosovan": "Kosovo", "kosovar": "Kosovo",
    "montenegrin": "Montenegro", "bulgarian": "Bulgaria", "greek": "Greece", "cypriot": "Cyprus",
    "maltese": "Malta", "austrian": "Austria", "swiss": "Switzerland", "belgian": "Belgium",
    "luxembourgish": "Luxembourg", "hungarian": "Hungary", "israeli": "Israel", "jordanian": "Jordan",
    "south african": "South Africa", "newzealand": "New Zealand", "new zealander": "New Zealand",
    "kiwi": "New Zealand", "canadian": "Canada", "american": "USA", "usa": "USA", "us": "USA", "usl": "USA", "usl league two": "USA",
    "barbados": "Barbados", "barbadian": "Barbados", "barbados premier": "Barbados", "barbados premier league": "Barbados",
    "dominican": "Dominican Republic", "dominicana": "Dominican Republic", "dominican republic": "Dominican Republic", "liga dominicana": "Dominican Republic", "liga dominicana de futbol": "Dominican Republic",
    "colombia": "Colombia", "categoria primera a": "Colombia", "categor a primera a": "Colombia", "primera a": "Colombia",
    "arg": "Argentina", "arg primera nacional": "Argentina", "primera nacional": "Argentina",
    "scottish premiership": "Scotland", "english premier": "England",
    "sweden division": "Sweden", "swedish division": "Sweden",
    "yemen league": "Yemen", "yemeni": "Yemen", "yemen league division": "Yemen",
    "j1": "Japan", "j2": "Japan", "j3": "Japan", "j2 j3": "Japan",
    "j league": "Japan", "100 year vision": "Japan", "100 year vision league": "Japan",
    "bra lp": "Brazil", "bra serie": "Brazil", "brasileiro": "Brazil",
    "zanzibar": "Tanzania", "zanzibar premier": "Tanzania", "zanzibar premier league": "Tanzania",
    "sand2": "South Africa", "sand 2": "South Africa", "safa sab": "South Africa",
    "eth": "Ethiopia", "ethio": "Ethiopia",
    "ind": "India", "isl": "Iceland",
    "tkm": "Turkmenistan", "turkmenistani": "Turkmenistan", "afghanistan": "Afghanistan", "afghan": "Afghanistan",
    "bangladeshi": "Bangladesh", "bangladesh": "Bangladesh",
    "nepali": "Nepal", "nepalese": "Nepal",
    "myanmar": "Myanmar", "burmese": "Myanmar",
    "laotian": "Laos", "laos": "Laos", "cambodian": "Cambodia",
    "bruneian": "Brunei",
    "mongolian": "Mongolia",
    "tibetan": "China",
    "maldivian": "Maldives",
    "srilankan": "Sri Lanka", "sri lankan": "Sri Lanka",
    "pakistani": "Pakistan",
    "rwandan": "Rwanda", "senegalese": "Senegal", "malian": "Mali",
    "ivorian": "Ivory Coast", "ivory coast": "Ivory Coast",
    "cameroonian": "Cameroon",
    "angolan": "Angola", "mozambican": "Mozambique",
    "namibian": "Namibia", "botswanan": "Botswana",
    "liberian": "Liberia", "sierra leonean": "Sierra Leone",
    "gambian": "Gambia", "guinean": "Guinea", "burkinabe": "Burkina Faso",
    "malawian": "Malawi", "swazi": "Eswatini", "lesotho": "Lesotho",
    "somali": "Somalia", "eritrean": "Eritrea", "djiboutian": "Djibouti",
    "libyan": "Libya",
    "cafa": "Afghanistan",
    "ofc": "Oceania",
    "cfa": "China",
    "national youth school football league": "China", "youth school football league": "China",
    "ningbo university": "China", "guizhou police academy": "China", "chongqing normal university": "China", "kashi university": "China",
    "afc": "Asia",
    "caf": "Africa",
    "concacaf": "Americas",
    "conmebol": "South America",
    "fifa": "International",
    "eredivisie": "Netherlands", "netherlands eredivisie": "Netherlands",
    "andorran": "Andorra", "andorra": "Andorra", "andorran primera divisio": "Andorra",
    "south australia reserve league": "Australia", "south australia": "Australia",
    "fijian": "Fiji", "fiji": "Fiji", "fijian national league": "Fiji",
    "chi liga de ascenso": "Chile", "chi liga de primera": "Chile",
    "lux l1 w": "Luxembourg",
    "ireland women s league": "Ireland", "ireland women's league": "Ireland",
    "curacao": "Curacao", "curacao liga mcb 1st division": "Curacao", "liga mcb 1st division": "Curacao",
    "ligapro serie a": "Ecuador",
}


LEAGUE_POWER_RULES: list[tuple[int, tuple[str, ...], tuple[str, ...]]] = [
    (1, (), ("uefa champions league", "champions league", "лига чемпионов")),
    (2, (), ("uefa europa league", "europa league", "лига европы")),
    (3, (), ("conference league", "лига конференций")),
    (4, (), ("copa libertadores", "libertadores")),
    (5, (), ("copa sudamericana", "sudamericana")),
    (10, ("england", "англия"), ("premier league", "epl", "apl", "апл", "премьер лига")),
    (11, ("spain", "испания"), ("la liga", "laliga", "primera division", "ла лига")),
    (12, ("italy", "италия"), ("serie a", "серия a", "серия а")),
    (13, ("germany", "германия"), ("bundesliga", "бундеслига")),
    (14, ("france", "франция"), ("ligue 1", "лига 1")),
    (20, ("netherlands", "нидерланды"), ("eredivisie", "эредивизи")),
    (21, ("portugal", "португалия"), ("primeira liga", "liga portugal")),
    (22, ("turkey", "турция"), ("super lig", "super league")),
    (23, ("belgium", "бельгия"), ("pro league", "first division a", "jupiler")),
    (24, ("scotland", "шотландия"), ("premiership", "premier league")),
    (40, ("brazil", "бразилия"), ("serie a", "brasileirao", "brasileiro serie a")),
    (41, ("argentina", "аргентина"), ("liga profesional", "primera division")),
    (42, ("usa", "united states", "сша"), ("major league soccer", "mls")),
    (43, ("mexico", "мексика"), ("liga mx",)),
    (44, ("colombia", "колумбия"), ("categoria primera a", "primera a")),
    (50, ("japan", "япония"), ("j1", "j1 league", "j league")),
    (51, ("south korea", "korea", "южная корея"), ("k league 1",)),
    (100, ("england", "англия"), ("championship",)),
    (101, ("spain", "испания"), ("segunda", "la liga 2", "laliga 2")),
    (102, ("italy", "италия"), ("serie b", "серия b", "серия б")),
    (103, ("germany", "германия"), ("2 bundesliga", "bundesliga 2")),
    (104, ("france", "франция"), ("ligue 2", "лига 2")),
    (107, ("brazil", "бразилия"), ("serie b", "brasileiro serie b")),
    (108, ("argentina", "аргентина"), ("primera nacional", "arg primera nacional")),
    (109, ("usa", "united states", "сша"), ("usl championship",)),
]


COUNTRY_POWER_ORDER = (
    "england", "англия", "spain", "испания", "italy", "италия", "germany", "германия", "france", "франция",
    "netherlands", "нидерланды", "portugal", "португалия", "turkey", "турция", "brazil", "бразилия",
    "argentina", "аргентина", "usa", "united states", "сша", "belgium", "бельгия", "scotland", "шотландия",
    "mexico", "мексика", "colombia", "колумбия", "japan", "япония", "south korea", "korea", "южная корея",
)


def _sort_text(value: Any) -> str:
    text = unicodedata.normalize("NFD", str(value or "").lower())
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.replace("ё", "е")
    text = re.sub(r"[^a-zа-я0-9]+", " ", text, flags=re.I).strip()
    return re.sub(r"\s+", " ", text)


def _has_phrase(text: str, phrase: str) -> bool:
    phrase_n = _sort_text(phrase)
    return bool(phrase_n) and f" {phrase_n} " in f" {_sort_text(text)} "


def country_power_rank(country: Any) -> int:
    c = _sort_text(country)
    for i, name in enumerate(COUNTRY_POWER_ORDER):
        if _has_phrase(c, name):
            return 300 + i
    return 900


def league_power_rank(country: Any, league: Any) -> int:
    c = _sort_text(country)
    l = _sort_text(league)
    for rank, countries, leagues in LEAGUE_POWER_RULES:
        if countries and not any(_has_phrase(c, x) for x in countries):
            continue
        if any(_has_phrase(l, x) for x in leagues):
            return rank
    low_text = f"{c} {l}"
    if any(_has_phrase(low_text, x) for x in ("women", "u19", "u20", "u21", "u23", "reserve", "youth", "amateur", "regional", "division 2", "division 3", "жен", "молод", "резерв")):
        return 1200 + country_power_rank(country)
    return country_power_rank(country)


def is_generic_region(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return not text or text in CONTINENT_NAMES


def looks_like_league_name(value: str) -> bool:
    low = str(value or "").strip().lower()
    return any(token in low for token in (
        "league", "division", "cup", "premier", "championship", "reserve", "women", "u17", "u19", "u20", "u21", "u22", "u23"
    ))


def country_code(country: str) -> str:
    text = str(country or "").strip()
    if len(text) == 2 and text.isalpha():
        return text.upper()
    low = text.lower()
    return RUS_COUNTRY_TO_CODE.get(low) or COUNTRY_CODE_MAP.get(low, "")


def country_name_from_text(*parts: Any) -> str:
    hay = " ".join(str(p or "") for p in parts).lower()
    hay = re.sub(r"[^a-z\s-]+", " ", hay)
    hay = re.sub(r"\s+", " ", hay).strip()
    hay_smushed = re.sub(r"[\s-]+", "", hay)
    for alias, country in sorted(COUNTRY_NAME_ALIASES.items(), key=lambda x: len(x[0]), reverse=True):
        alias_low = alias.lower()
        pattern = r"(^|\s)" + re.escape(alias_low) + r"(\s|$)"
        if re.search(pattern, hay):
            return country
        if " " in alias_low or "-" in alias_low:
            if re.search(r"(^|[^a-z])" + re.escape(re.sub(r"[\s-]+", "", alias_low)) + r"([^a-z]|$)", hay_smushed):
                return country
    for name in sorted(COUNTRY_CODE_MAP.keys(), key=len, reverse=True):
        name_low = name.lower()
        pattern = r"(^|\s)" + re.escape(name_low) + r"(\s|$)"
        if re.search(pattern, hay):
            return " ".join(w.capitalize() for w in name.split())
        if " " in name_low:
            smushed = re.sub(r"\s+", "", name_low)
            if re.search(r"(^|[^a-z])" + re.escape(smushed) + r"([^a-z]|$)", hay_smushed):
                return " ".join(w.capitalize() for w in name.split())
    return ""
