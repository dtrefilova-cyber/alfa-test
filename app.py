import streamlit as st
import pandas as pd
import requests
import json
import re
import os
from google_sheets import (
    append_log_info,
    append_manager_log,
    append_qa_log,
    connect_google,
    load_managers_config,
    write_to_google_sheet,
)
from io import BytesIO
from datetime import datetime
from openai import OpenAI
from prompts import get_full_analysis_prompt_claude, get_full_analysis_prompt_openai
import anthropic

# ================= CONFIG =================
def read_secret(name, default=None):
    value = st.secrets.get(name)
    if value is None or str(value).strip() == "":
        env_value = os.getenv(name)
        if env_value is not None and str(env_value).strip() != "":
            return env_value
        return default
    return value


DEEPGRAM_API_KEY = read_secret("DEEPGRAM_API_KEY")
OPENAI_API_KEY = read_secret("OPENAI_API_KEY")
ANTHROPIC_API_KEY = read_secret("ANTHROPIC_API_KEY")

missing_required = []
if not DEEPGRAM_API_KEY:
    missing_required.append("DEEPGRAM_API_KEY")
if not OPENAI_API_KEY:
    missing_required.append("OPENAI_API_KEY")

if missing_required:
    st.error(
        "Відсутні обов'язкові секрети: "
        + ", ".join(missing_required)
        + ". Додайте їх у Streamlit Secrets (або environment variables) і перезапустіть застосунок."
    )
    st.stop()

client = OpenAI(api_key=OPENAI_API_KEY)

claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None

LOG_SHEET_ID = "1gElj3hB5CX86YsVQFG2M9DpfvMUMPq2lfuSNj-ylN94"
DICT_SHEET_ID = "1gElj3hB5CX86YsVQFG2M9DpfvMUMPq2lfuSNj-ylN94"
KB_SHEET_ID = "1yZbtao1P1Xa0r6ZJAnjkJWikxcWQ90XbXvaT7EWQKeU"
ANALYSIS_CACHE_VERSION = "2026-04-23-2"
OPENAI_ANALYSIS_MODEL = st.secrets.get("OPENAI_MODEL", "gpt-5.4-mini")
CLAUDE_ANALYSIS_MODEL = st.secrets.get("CLAUDE_MODEL", "claude-sonnet-4-20250514")
OPENAI_TRANSCRIPT_MODEL = st.secrets.get("OPENAI_TRANSCRIPT_MODEL", "gpt-4o-mini")
OPENAI_MAX_OUTPUT_TOKENS = int(st.secrets.get("OPENAI_MAX_OUTPUT_TOKENS", 2200))
CLAUDE_MAX_OUTPUT_TOKENS = int(st.secrets.get("CLAUDE_MAX_OUTPUT_TOKENS", 2200))

# ================= HEADER =================
st.markdown("""
<div class="card">
    <h2 style="margin:0;">🎧 QA-10</h2>
    <span style="color:#aaa;">Аналіз дзвінків</span>
</div>
""", unsafe_allow_html=True)

check_date = st.date_input("Дата перевірки", datetime.today())

qa_managers_list = [
    "Дар'я", "Надя", "Настя", "Владимира", "Діана", "Руслана", "Олексій", "Катерина"
]

FORBIDDEN_PROFESSIONALISM_PHRASES = [
    "Лотерея",
    "Акція",
    "Розіграш",
    "Реклама",
    "Подарунок",
    "Популяризація",
    "Лотерейний білет",
    "Даруємо",
    "Розігруємо",
    "Конкурс",
    "Кешбек",
    "Відшкодуємо",
    "Компенсація",
    "Повернення",
    "Фріспіни",
    "Безкоштовно",
    "Страхування",
    "страховка",
    "ставка без ризику",
    "фрібет",
    "Бездеп",
]

call_completion_statuses = [
    "⚪ (відсутній статус)",
    "🟢 (слухавку поклав клієнт)",
    "🟡 (технічні проблеми, зв'язок обірвався)",
    "🔴 (слухавку поклав менеджер)",
]

@st.cache_data(ttl=300, show_spinner=False)
def get_managers_config():
    google_client = connect_google()
    return load_managers_config(google_client, LOG_SHEET_ID)


@st.cache_data(ttl=300, show_spinner=False)
def get_reference_data():
    google_client = connect_google()
    dict_sheet = google_client.open_by_key(LOG_SHEET_ID).worksheet("DICT")
    replacements = load_replacements(dict_sheet)

    kb_sheet = google_client.open_by_key(KB_SHEET_ID).worksheet("INFO")
    kb_data = load_kb_data(kb_sheet)
    kb_context = build_kb_context(kb_data)
    return replacements, kb_data, kb_context


managers_meta = {
    "headers": [],
    "header_row_index": None,
    "raw_rows_count": 0,
    "valid_rows_count": 0
}

try:
    managers_payload = get_managers_config()
    managers_config = managers_payload.get("managers", [])
    managers_meta = {
        "headers": managers_payload.get("headers", []),
        "header_row_index": managers_payload.get("header_row_index"),
        "raw_rows_count": managers_payload.get("raw_rows_count", 0),
        "valid_rows_count": managers_payload.get("valid_rows_count", 0)
    }
except Exception as e:
    managers_config = []
    st.error(f"Помилка завантаження менеджерів: {e}")

projects_list = sorted({item["project"] for item in managers_config})

if not managers_config:
    st.warning(
        "Список проєктів і менеджерів не завантажився з аркуша MANAGERS. "
        "Перевірте, що в аркуші є заголовки MANAGERS_NAME, PROJECT, SHEET_ID "
        "і що в колонці SHEET_ID заповнені значення."
    )
    st.caption(
        f"Діагностика: headers={managers_meta['headers']}, "
        f"header_row={managers_meta['header_row_index']}, "
        f"raw_rows={managers_meta['raw_rows_count']}, "
        f"valid_rows={managers_meta['valid_rows_count']}"
    )

# ================= INPUT =================
calls = []
for row in range(5):
    col1, col2 = st.columns(2)
    for col, idx in zip([col1, col2], [row * 2 + 1, row * 2 + 2]):
        with col.expander(f"📞 Дзвінок {idx}"):
            audio_url = st.text_input("Посилання", key=f"url_{idx}")
            qa_manager = st.selectbox("QA", qa_managers_list, key=f"qa_{idx}")
            selected_project = st.selectbox(
                "Проєкт",
                projects_list,
                index=None,
                placeholder="Оберіть проєкт",
                key=f"project_{idx}",
                disabled=not projects_list
            )
            project_managers = [
                item for item in managers_config
                if item["project"] == selected_project
            ]
            manager_names = [item["manager_name"] for item in project_managers]
            selected_manager = st.selectbox(
                "Менеджер РЕТ",
                manager_names,
                index=None,
                placeholder="Оберіть менеджера",
                key=f"ret_{idx}",
                disabled=not manager_names
            )
            selected_manager_data = next(
                (item for item in project_managers if item["manager_name"] == selected_manager),
                None
            )
            client_id = st.text_input("ID", key=f"client_{idx}")
            call_date = st.text_input("Дата", key=f"date_{idx}")
            bonus_check = st.selectbox(
                "Бонус",
                ["правильно нараховано", "помилково нараховано", "не потрібно"],
                key=f"bonus_{idx}"
            )
            repeat_col, completion_col = st.columns(2)
            with repeat_col:
                repeat_call = st.selectbox(
                    "Передзвон",
                    ["так, був протягом години", "так, був протягом 2 годин", "ні, не було"],
                    key=f"repeat_{idx}"
                )
            with completion_col:
                call_completion_status = st.selectbox(
                    "Завершення виклику",
                    call_completion_statuses,
                    key=f"call_completion_{idx}"
                )
            manager_comment = st.text_area("Коментар", key=f"comment_{idx}")

            calls.append({
                "url": audio_url.strip(),
                "qa_manager": qa_manager,
                "project": selected_project or "",
                "ret_manager": selected_manager or "",
                "ret_sheet_id": selected_manager_data["sheet_id"] if selected_manager_data else "",
                "client_id": client_id,
                "call_date": call_date,
                "check_date": check_date.strftime("%d-%m-%Y"),
                "bonus_check": bonus_check,
                "repeat_call": repeat_call,
                "call_completion_status": call_completion_status,
                "manager_comment": manager_comment,
            })

# ================= TRANSCRIPTION =================
def post_process_transcript(text: str) -> str:
    """Базова локальна нормалізація транскрипту Deepgram до застосування словника DICT.
    Працює лише з відомими патернами злипань у запереченнях, щоб не зіпсувати правильні слова."""
    if not text:
        return text

    text = re.sub(r" {2,}", " ", text)

    negation_fixes = [
        (r"\bненайд", "не найд"),
        (r"\bнемож", "не мож"),
        (r"\bнехоч", "не хоч"),
        (r"\bнезруч", "не зруч"),
        (r"\bнепотріб", "не потріб"),
    ]
    for pattern, replacement in negation_fixes:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

    return text


def build_keyterms(kb_data, managers_config, max_tokens=450):
    """Зібрати список keyterms для Deepgram із трьох джерел із жорстким лімітом
    сумарної кількості токенів (Deepgram обмежує ~500).

    Deepgram використовує BPE-токенізатор: для кирилиці 1 слово ≈ 4-6 BPE-токенів,
    тому оцінюємо вартість за кількістю символів (консервативно ~2 симв/токен).

    Пріоритет: 1) статична проф.лексика, 2) NAME з KB_SHEET, 3) імена менеджерів,
    4) аліаси з KB_SHEET (додаються останніми, скільки влізе)."""
    result = []
    token_count = 0

    def estimate_tokens(term):
        # ~2 символи на 1 Deepgram BPE-токен (консервативна оцінка для кирилиці).
        return max(1, (len(term) + 1) // 2)

    def try_add(term):
        nonlocal token_count
        term = (term or "").strip()
        if not term or len(term) < 3:
            return
        tokens = estimate_tokens(term)
        if token_count + tokens > max_tokens:
            return
        if term not in result:
            result.append(term)
            token_count += tokens

    static_terms = [
        "фріспін", "фриспін", "кешбек", "бездепозитний",
        "вейджер", "відіграш", "депозит", "нарахування",
        "бонус від менеджера", "захист ставки", "мінімальний пакет",
        "програма лояльності", "особистий кабінет",
    ]
    for t in static_terms:
        try_add(t)

    for row in kb_data or []:
        name = str(row.get("NAME", "")).strip()
        if name:
            try_add(name)

    for item in managers_config or []:
        name = str(item.get("manager_name", "")).strip()
        if name:
            try_add(name.split()[0])

    for row in kb_data or []:
        aliases = str(row.get("ALIASES", "")).strip()
        for alias in aliases.split(";"):
            try_add(alias)

    return tuple(result)


@st.cache_data(ttl=86400, show_spinner=False)
def transcribe_audio_cached(url, keyterms=()):
    if not url:
        return {"ok": False, "error": "empty url", "transcript": None}

    try:
        base_params = {
            "model": "nova-3",
            "smart_format": "true",
            "punctuate": "true",
            "utterances": "true",
            "multichannel": "true",
            "diarize": "true",
            "language": "uk",
        }
        keyterm_params = [("keyterm", t) for t in (keyterms or ())]

        r = requests.post(
            "https://api.deepgram.com/v1/listen",
            headers={"Authorization": f"Token {DEEPGRAM_API_KEY}"},
            params=list(base_params.items()) + keyterm_params,
            json={"url": url}
        )

        if r.status_code != 200:
            return {"ok": False, "error": f"Deepgram error: {r.text}", "transcript": None}

        data = r.json()
        results = data.get("results", {})

        channels = results.get("channels", [])
        utterances = results.get("utterances", [])

        all_words = []

        if not channels and utterances:
            dialogue = []
            for u in utterances:
                speaker = f"ch_{u.get('speaker', 0)}"
                text = u.get("transcript", "")
                if text:
                    dialogue.append(f"{speaker}: {text}")
            transcript_text = post_process_transcript("\n".join(dialogue))
            return {"ok": True, "error": "", "transcript": transcript_text}

        for ch_index, ch in enumerate(channels):
            alternatives = ch.get("alternatives", [])
            if not alternatives:
                continue

            words = alternatives[0].get("words", [])

            for w in words:
                all_words.append({
                    "word": w.get("word", ""),
                    "start": w.get("start", 0),
                    "end": w.get("end", 0),
                    "speaker": f"ch_{ch_index}"
                })

        if not all_words:
            return {"ok": False, "error": "Немає транскрипції", "transcript": None}

        all_words.sort(key=lambda x: x["start"])

        dialogue = []
        current_speaker = all_words[0]["speaker"]
        current_phrase = []
        last_end = all_words[0]["end"]

        for w in all_words:
            speaker = w["speaker"]
            pause = w["start"] - last_end

            if speaker != current_speaker or pause > 0.5:
                if current_phrase:
                    dialogue.append(f"{current_speaker}: {' '.join(current_phrase)}")

                current_phrase = []
                current_speaker = speaker

            current_phrase.append(w["word"])
            last_end = w["end"]

        if current_phrase:
            dialogue.append(f"{current_speaker}: {' '.join(current_phrase)}")

        transcript_text = post_process_transcript("\n".join(dialogue))
        return {"ok": True, "error": "", "transcript": transcript_text}

    except Exception as e:
        return {"ok": False, "error": f"Transcription exception: {str(e)}", "transcript": None}


def transcribe_audio(url, keyterms=()):
    result = transcribe_audio_cached(url, keyterms=tuple(keyterms))
    if not result["ok"]:
        st.error(result["error"])
        return None
    return result["transcript"]


@st.cache_data(ttl=86400, show_spinner=False)
def clean_transcript_cached(raw_transcript, cache_version):
    if not raw_transcript:
        return raw_transcript
    try:
        res = client.chat.completions.create(
            model=OPENAI_TRANSCRIPT_MODEL,
            temperature=0,
            max_completion_tokens=3000,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ти — редактор транскриптів телефонних дзвінків. "
                        "Твоє завдання — виправити транскрипт не змінюючи змісту розмови.\n\n"
                        "Правила:\n"
                        "1. Виправ очевидні помилки розпізнавання: злипання слів, спотворені слова, фонетичні заміни\n"
                        "2. Перейменуй спікерів: ch_0 → Менеджер, ch_1 → Клієнт\n"
                        "3. Збережи формат діалогу: кожна репліка з нового рядка у форматі 'Спікер: текст'\n"
                        "4. Не додавай, не прибирай і не перефразовуй репліки — тільки виправляй помилки\n"
                        "5. Поверни тільки виправлений транскрипт без коментарів"
                    )
                },
                {
                    "role": "user",
                    "content": raw_transcript
                }
            ]
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        st.warning(f"Помилка обробки транскрипту: {e}")
        return raw_transcript


if st.button("🗑️ Скинути кеш транскрипцій", type="secondary"):
    transcribe_audio_cached.clear()
    clean_transcript_cached.clear()
    st.success("Кеш транскрипцій очищено")


# ================= DICT =================
def normalize_sheet_headers(row):
    return {
        str(key).strip().upper(): value
        for key, value in row.items()
    }


def load_replacements(sheet):
    try:
        data = [normalize_sheet_headers(row) for row in sheet.get_all_records()]
        return {
            str(row["RAW"]).strip(): str(row["CORRECT"]).strip()
            for row in data
            if row.get("RAW") and row.get("CORRECT")
        }
    except Exception:
        return {}


def load_kb_data(sheet):
    try:
        return [normalize_sheet_headers(row) for row in sheet.get_all_records()]
    except Exception:
        return []


def apply_replacements(text, replacements):
    if not text:
        return text

    for k, v in replacements.items():
        pattern = re.compile(rf"(?<!\w){re.escape(k)}(?!\w)", re.IGNORECASE)
        if pattern.search(text):
            text = pattern.sub(v, text)

    return text

def detect_presentation(dialogue, kb_data):
    if not dialogue:
        return False

    manager_lines = []
    for line in str(dialogue).splitlines():
        stripped = line.strip()
        if stripped.startswith("Менеджер:") or stripped.startswith("ch_0:"):
            manager_lines.append(stripped.split(":", 1)[1].strip())

    text = " ".join(manager_lines).lower()
    if not text:
        return False

    for row in kb_data:
        name = (row.get("NAME") or "").lower()
        aliases = (row.get("ALIASES") or "").lower().split(";")

        variants = [name] + aliases

        for v in variants:
            v = v.strip()
            if v and v in text:
                return True

    return False


def extract_role_lines(dialogue):
    manager_lines = []
    client_lines = []

    for raw_line in str(dialogue or "").splitlines():
        stripped = raw_line.strip()
        if ":" not in stripped:
            continue

        speaker, text = stripped.split(":", 1)
        speaker = speaker.strip().lower()
        text = text.strip()
        if not text:
            continue

        if speaker in {"менеджер", "ch_0"}:
            manager_lines.append(text)
        elif speaker in {"клієнт", "клиент", "ch_1"}:
            client_lines.append(text)

    return manager_lines, client_lines


def has_any_marker(text, markers):
    normalized = f" {str(text or '').lower()} "
    return any(marker in normalized for marker in markers)


def normalize_presentation_level(features, dialogue, kb_data):
    """
    Презентація визначається ТІЛЬКИ кодом через KB.
    LLM не бере участі у визначенні presentation_level.
    Правило: є продукт/активність/лояльність з KB → full, інакше → none.
    """
    manager_lines, _ = extract_role_lines(dialogue)
    manager_text = " ".join(manager_lines).lower()

    if not manager_text:
        features["presentation_level"] = "none"
        return features

    # --- Перевірка 1: продукт або активність з KB ---
    has_product_mention = detect_presentation(dialogue, kb_data)

    # --- Перевірка 2: програма лояльності (монетки, медалі тощо) ---
    loyalty_markers = [
        "монет",
        "медал",
        "програм лояльн",
        "програма лояльності",
        "рівень лояльності",
    ]
    has_loyalty_mention = has_any_marker(manager_text, loyalty_markers)

    # --- Виключення: бонусний контекст без продукту ---
    # Якщо є явні ознаки бонусу від менеджера — перевіряємо чи є продукт поруч
    bonus_only_markers = [
        "від себе",
        "від менеджера",
        "залишу бонус",
        "залишаю бонус",
        "залишив бонус",
        "залишила бонус",
        "нарахую бонус",
        "бонус від менеджера",
    ]
    has_bonus_offer = has_any_marker(manager_text, bonus_only_markers)

    # Якщо є тільки бонус без продукту і без лояльності — не презентація
    if has_bonus_offer and not has_product_mention and not has_loyalty_mention:
        features["presentation_level"] = "none"
        return features

    # --- Основне правило ---
    if has_product_mention or has_loyalty_mention:
        features["presentation_level"] = "full"
    else:
        features["presentation_level"] = "none"

    return features


def build_kb_context(kb_data):
    lines = []

    for row in kb_data:
        name = str(row.get("NAME", "")).strip()
        aliases = str(row.get("ALIASES", "")).strip()

        if not name:
            continue

        parts = [f"Продукт: {name}"]
        if aliases:
            parts.append(f"Аліаси: {aliases}")

        lines.append(" | ".join(parts))

    return "\n".join(lines)


# ================= CLEAN =================
def is_autoresponder(dialogue: str) -> bool:
    if not dialogue:
        return False

    text = dialogue.lower()

    triggers = [
        "залиште повідомлення",
        "після сигналу",
        "абонент недоступний",
        "не може відповісти",
        "voice mail",
        "voicemail",
        "please leave a message",
        "номер не обслуговується"
    ]

    return any(t in text for t in triggers)

# ================= GPT =================
def apply_defaults(features):
    defaults = {
        "manager_name_present": False,
        "manager_position_present": False,
        "company_present": False,
        "client_name_used": False,
        "purpose_present": False,
        "friendly_question": False,
        "noise_reaction": "none",

        "bonus_offered": False,
        "bonus_has_type": False,
        "bonus_has_duration": False,
        "bonus_has_value": False,

        "followup_type": "none",

        "objection_detected": False,
        "client_wants_to_end": False,
        "continuation_level": "none",
        "continuation_behavior": "neutral",

        "has_farewell": False,
        "is_limited_dialogue": False,

        "presentation_level": "none",
        "speech_quality": "bad",
        "forbidden_words_used": False,
        "forbidden_words_detected": [],
        "conversation_logically_completed": False,
        "client_negative": False,
        "client_used_profanity": False,
        "manager_hung_up_before_client_finished": False,

        "assumption_made": False,
        "assumption_soft": False,
        "followup_attempts_count": 0,
        "client_hung_up_interrupted": False,
        "client_sick": False,
        "manager_wished_recovery": False,
        "client_military": False,
        "manager_thanked_for_service": False,
        "client_driving_or_no_phone": False,
        "client_not_actual_client": False,
        "manager_shared_bonus_with_third_party": False,
        "client_unethical_behavior": False,
        "manager_unethical_response": False,

        "comment_match_level": "none",
        "comment_complete": False,
        "card_has_reason": False,
        "card_has_followup_time": False
    }

    for k, v in defaults.items():
        features.setdefault(k, v)

    return features


def normalize_forbidden_phrase(text):
    normalized = str(text or "").strip().lower()
    normalized = normalized.replace("’", "'").replace("`", "'").replace("ё", "е")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def detect_forbidden_phrases_in_dialogue(dialogue):
    if not dialogue:
        return []

    manager_lines = []
    for line in str(dialogue).splitlines():
        stripped = line.strip()
        if stripped.startswith("Менеджер:") or stripped.startswith("ch_0:"):
            manager_lines.append(stripped.split(":", 1)[1].strip())

    manager_text = " ".join(manager_lines)
    if not manager_text:
        return []

    normalized_text = normalize_forbidden_phrase(manager_text)
    detected = []

    for phrase in FORBIDDEN_PROFESSIONALISM_PHRASES:
        normalized_phrase = normalize_forbidden_phrase(phrase)
        if not normalized_phrase:
            continue

        if " " in normalized_phrase:
            matched = normalized_phrase in normalized_text
        else:
            matched = re.search(rf"(?<!\w){re.escape(normalized_phrase)}(?!\w)", normalized_text) is not None

        if matched:
            detected.append(phrase)

    return detected


def validate_forbidden_words(features, dialogue):
    detected = detect_forbidden_phrases_in_dialogue(dialogue)
    features["forbidden_words_detected"] = detected
    features["forbidden_words_used"] = bool(detected)
    return features


def validate_friendly_question(features, dialogue):
    """Виключає питання про сайт/продукт як хибні дружні питання.

    Дружнє питання має стосуватись особисто клієнта (справи, настрій, життя),
    а не сайту, гри чи наявних у клієнта питань по продукту.
    """
    if not features.get("friendly_question"):
        return features

    manager_lines, _ = extract_role_lines(dialogue)
    manager_text = " ".join(manager_lines).lower()
    if not manager_text:
        return features

    real_friendly_patterns = [
        r"як\s+(?:ваші\s+|твої\s+)?справи(?!\s+(?:на\s+сайт|по\s+сайт|з\s+сайт))",
        r"як\s+настрій",
        r"як\s+(?:ваше?\s+)?життя",
        r"як\s+ви\b(?!\s+там\s+на\s+сайт)",
        r"як\s+почуває",
        r"як\s+себе\s+почува",
        r"як\s+ваш\s+день",
        r"як\s+вихідн",
    ]

    has_real_friendly = any(re.search(p, manager_text) for p in real_friendly_patterns)

    if not has_real_friendly:
        features["friendly_question"] = False

    return features


def validate_assumption_made(features, dialogue):
    manager_lines, client_lines = extract_role_lines(dialogue)
    manager_text = " ".join(manager_lines).lower()
    client_text = " ".join(client_lines).lower()
    if not manager_text:
        features["assumption_made"] = False
        features["assumption_soft"] = False
        return features

    soft_assumption_markers = [
        "не відволікаю",
        "не заважаю",
        "не відриваю",
        "чи зручно говорити",
        "чи зручно вам говорити",
        "я вам не заважаю",
        "не дуже вчасно набрав",
        "що відволікаю",
        "що заважаю",
        "що турбую",
        "що потурбувала",
        "що потурбував",
        "зручно вам зараз",
        "зручно зараз",
        "зараз зручно",
        "чи зручно",
    ]

    hard_assumption_markers = [
        "вам зараз незручно",
        "давайте іншим разом",
        "ви, мабуть, зайняті",
        "ви мабуть зайняті",
        "немає часу так спілкуватися",
        "вам, мабуть, нецікаво",
        "вам, мабуть, незручно",
        "вам, мабуть, не до розмови",
        "вам мабуть нецікаво",
        "вам мабуть незручно",
        "вам мабуть не до розмови",
        "вам незручно",
        "ви зайняті",
    ]

    client_state_markers = [
        "я зайнятий",
        "я занята",
        "мені незручно",
        "не можу говорити",
        "я за кермом",
        "передзвоніть",
    ]

    has_hard = any(marker in manager_text for marker in hard_assumption_markers)
    has_soft = any(marker in manager_text for marker in soft_assumption_markers)

    # Якщо клієнт уже сигналізував про стан — менеджер реагує, а не додумує
    client_already_signaled = any(marker in client_text for marker in client_state_markers)

    if client_already_signaled:
        features["assumption_made"] = False
        features["assumption_soft"] = False
    elif has_hard:
        features["assumption_made"] = True
        features["assumption_soft"] = False
    elif has_soft:
        features["assumption_made"] = True
        features["assumption_soft"] = True
    else:
        features["assumption_made"] = False
        features["assumption_soft"] = False

    return features


def validate_bonus_features(features, dialogue):
    manager_lines, _ = extract_role_lines(dialogue)
    manager_lines_lc = [line.lower() for line in manager_lines]
    manager_text = " ".join(manager_lines_lc)

    if not manager_text:
        return features

    # Override: якщо менеджер не вжив жодного явного бонус-індикатора —
    # ані слова "бонус", ані назви типу бонусу (fs/фріспін/кешбек/бездеп/фрібет/захист ставки),
    # ані фрази-оферти від себе ("від себе", "від менеджера") — пропозиції бонусу НЕ було.
    # Описи активностей/акцій сайту (Happy Hours, щасливі години, турніри, програми лояльності)
    # з "%" / "до депозиту" / "без відіграшу" — це НЕ бонус.
    explicit_bonus_indicators = [
        "бонус",
        "фс",
        "fs",
        "фріспін",
        "фриспін",
        "кешбек",
        "бездеп",
        "фрібет",
        "захист ставк",
        "від себе",
        "від менеджера",
    ]
    has_explicit_bonus = has_any_marker(manager_text, explicit_bonus_indicators)
    if not has_explicit_bonus:
        features["bonus_offered"] = False
        features["bonus_has_type"] = False
        features["bonus_has_duration"] = False
        features["bonus_has_value"] = False
        return features

    bonus_topic_markers = [
        "бонус",
        "фс",
        "fs",
        "фріспін",
        "фриспін",
        "кешбек",
        "бездеп",
        "фрібет",
        "вейдж",
        "відіграш",
        "оберт",
        "крути",
    ]
    offer_markers = [
        "нарахую бонус",
        "нараховано бонус",
        "бонус нарахував",
        "бонус нарахувала",
        "вам бонус нарахував",
        "вам бонус нарахувала",
        "дам бонус",
        "буде бонус",
        "будуть бонуси",
        "залишу бонус",
        "залишаю бонус",
        "залишив бонус",
        "залишила бонус",
        "лишив бонус",
        "лишила бонус",
        "бонус залишу",
        "доступний бонус",
        "бонус від менеджера",
        "від себе бонус",
        "від себе залишаю бонус",
        "подарую бонус",
        "отримаєте бонус",
        "отримаєш бонус",
        "можу дати бонус",
        "можна залишу бонус",
        "дозвольте залишу бонус",
        "хочу залишити бонус",
        "щоб бонус вам залишила",
        "щоб бонус вам залишив",
        "щоб залишити вам бонус",
    ]
    type_markers = [
        "фс",
        "fs",
        "фріспін",
        "фриспін",
        "спін",
        "спини",
        "кешбек",
        "кешбеку",
        "бонус на депозит",
        "бездеп",
        "фрібет",
        "від менеджера",
        "від себе",
        "захист став",
        "захист ставки",
        "захист ставці",
    ]
    duration_markers = [
        "годин",
        "днів",
        "день",
        "тиж",
        "до кінця",
        "сьогодні",
        "завтра",
        "48",
        "24",
        "термін дії",
        "діє",
    ]
    value_markers = [
        "%",
        "відсот",
        "грн",
        "грив",
        "сума",
        "депозит",
        "поповнен",
        "вейдж",
        "відіграш",
        "ставк",
    ]

    bonus_line_indexes = [
        idx for idx, line in enumerate(manager_lines_lc)
        if has_any_marker(line, bonus_topic_markers)
    ]
    expanded_indexes = set()
    for idx in bonus_line_indexes:
        expanded_indexes.add(idx)
        if idx > 0:
            expanded_indexes.add(idx - 1)
        if idx + 1 < len(manager_lines_lc):
            expanded_indexes.add(idx + 1)

    bonus_lines = [
        line for idx, line in enumerate(manager_lines_lc)
        if idx in expanded_indexes
    ]
    bonus_text = " ".join(bonus_lines)

    if not bonus_text:
        return features

    has_multiplier_value = re.search(r"(?<!\w)\d+\s*[xх](?!\w)", bonus_text) is not None
    has_stake_range = re.search(r"став\w*\s+від\s+\S+\s+до\s+\S+", bonus_text) is not None
    has_offer_regex = (
        re.search(r"нарах\w*[^.]{0,40}бонус|бонус[^.]{0,40}нарах\w*", bonus_text) is not None
        or re.search(r"(залиш|лиш)\w*[^.]{0,40}бонус|бонус[^.]{0,40}(залиш|лиш)\w*", bonus_text) is not None
        or re.search(r"від себе[^.]{0,40}бонус|бонус[^.]{0,40}від менеджера", bonus_text) is not None
    )
    detected_type = has_any_marker(bonus_text, type_markers)
    detected_duration = has_any_marker(bonus_text, duration_markers)
    detected_value = (
        has_any_marker(bonus_text, value_markers)
        or has_multiplier_value
        or has_stake_range
    )
    has_offer = (
        has_any_marker(bonus_text, offer_markers)
        or has_offer_regex
        or ("бонус" in bonus_text and (detected_type or detected_duration or detected_value))
    )

    if not has_offer:
        if not features.get("bonus_offered"):
            features["bonus_has_type"] = False
            features["bonus_has_duration"] = False
            features["bonus_has_value"] = False
        return features

    features["bonus_offered"] = True
    features["bonus_has_type"] = bool(features.get("bonus_has_type")) or detected_type
    features["bonus_has_duration"] = bool(features.get("bonus_has_duration")) or detected_duration
    features["bonus_has_value"] = bool(features.get("bonus_has_value")) or detected_value
    return features


def validate_card_features(features):
    # Якщо домовленості про наступний контакт не було або клієнт сам обірвав дзвінок,
    # час передзвону у коментарі не є обов'язковим елементом.
    followup_none = features.get("followup_type", "none") == "none"
    client_hung_up = bool(features.get("client_hung_up_interrupted"))
    if (followup_none or client_hung_up) and features.get("card_has_reason"):
        features["card_has_followup_time"] = True
    return features


def validate_card_reason(features, manager_comment):
    """Незалежна перевірка коментаря менеджера на наявність причини завершення.
    Спрацьовує поверх рішення АІ — якщо АІ не розпізнав через помилки транскрипту,
    маркери виправлять."""
    if features.get("card_has_reason"):
        return features

    comment = str(manager_comment or "").lower()
    if not comment.strip():
        return features

    reason_markers = [
        "не мож",
        "не міг",
        "не може",
        "не могла",
        "не можу",
        "не зміг",
        "не змогла",
        "не можна",
        "не буду",
        "зайнят",
        "занят",
        "розмовляє",
        "на роботі",
        "не зручно",
        "незручно",
        "працює",
        "сервіс",
        "все ок",
        "все добре",
        "задоволен",
        "скинув",
        "скинула",
        "поклав",
        "поклала",
        "не до розмови",
        "не до телефону",
        "за кермом",
        "не відповів",
        "не відповіла",
        "недоступн",
        "автовідповідач",
        "сброс",
        "сбросил",
    ]

    if any(marker in comment for marker in reason_markers):
        features["card_has_reason"] = True

    return features


def validate_card_followup_time(features, manager_comment):
    """
    Незалежна перевірка коментаря на наявність часу наступного контакту.
    Спрацьовує поверх рішення АІ.
    """
    if features.get("card_has_followup_time"):
        return features

    comment = str(manager_comment or "").lower()
    if not comment.strip():
        return features

    if re.search(r"\d{1,2}[:\.\-]\d{2}", comment):
        features["card_has_followup_time"] = True
        return features

    time_markers = [
        "завтра",
        "після",
        "перезвон",
        "передз",
        "через годину",
        "через дві",
        "ввечері",
        "вранці",
        "вдень",
        "пізніше",
        "наберу",
        "передзвоню",
    ]
    if any(marker in comment for marker in time_markers):
        features["card_has_followup_time"] = True

    return features


def validate_followup_type(features, dialogue):
    """Знижує followup_type з 'exact_time' до 'offer', якщо у репліках менеджера
    немає конкретного часу (18:00, о 18, після 17, через 15 хвилин),
    а є лише розмиті формулювання (ввечері, пізніше, після роботи)."""
    manager_lines, _ = extract_role_lines(dialogue)
    manager_text = " ".join(manager_lines).lower()
    if not manager_text:
        return features

    _, client_lines = extract_role_lines(dialogue)
    client_text = " ".join(client_lines).lower()

    manager_has_approx_exact_time = any(
        re.search(pattern, manager_text)
        for pattern in [
            r"\bближче\s+до?\s*\d{1,2}\b",
            r"\bближче\s+\d{1,2}\b",
            r"\bпісля\s+\d{1,2}\b",
            r"\bпісля\s+\d{1,2}\s*год",
        ]
    )
    client_confirmed_followup = has_any_marker(
        client_text,
        ["добре", "дякую", "ага", "домовились", "окей", "добре, все"],
    )

    # "ближче до X", "після X" + підтвердження клієнта = exact_time.
    if features.get("followup_type") in {"none", "offer"}:
        if manager_has_approx_exact_time and client_confirmed_followup:
            features["followup_type"] = "exact_time"
        return features

    if features.get("followup_type") != "exact_time":
        return features

    exact_time_patterns = [
        r"\b\d{1,2}[:\.\-]\d{2}\b",
        r"\bо\s+\d{1,2}\b",
        r"\bпісля\s+\d{1,2}\b",
        r"\bближче\s+до?\s*\d{1,2}\b",
        r"\bближче\s+\d{1,2}\b",
        r"\bдо\s+\d{1,2}\b",
        r"через\s+\d+\s*(хвилин|годин)",
        r"через\s+пів\s*години",
        r"через\s+півгодини",
    ]
    has_exact_time = any(re.search(p, manager_text) for p in exact_time_patterns)
    if has_exact_time:
        return features

    vague_time_markers = [
        "ввечері",
        "увечері",
        "увечорі",
        "вранці",
        "зранку",
        "вдень",
        "після роботи",
        "пізніше",
        "трошки пізніше",
        "трохи пізніше",
        "згодом",
        "пополудні",
    ]
    has_vague_time = any(marker in manager_text for marker in vague_time_markers)
    if has_vague_time:
        features["followup_type"] = "offer"

    return features


def validate_professionalism_features(features, dialogue):
    manager_lines, client_lines = extract_role_lines(dialogue)
    manager_text = " ".join(manager_lines).lower()
    client_text = " ".join(client_lines).lower()

    direct_client_markers = [
        "ви",
        "вам",
        "вас",
        "з вами",
    ]
    third_party_markers = [
        "це не",
        "його немає",
        "її немає",
        "мама",
        "тато",
        "дружина",
        "чоловік",
        "син",
        "донька",
        "дочка",
        "брат",
        "сестра",
        "подруга",
    ]

    has_direct_client_communication = has_any_marker(manager_text, direct_client_markers)
    has_clear_third_party_context = has_any_marker(client_text, third_party_markers) or has_any_marker(manager_text, third_party_markers)

    if has_direct_client_communication or not has_clear_third_party_context:
        features["client_not_actual_client"] = False

    return features


def validate_dialogue_exceptions(features, dialogue):
    _, client_lines = extract_role_lines(dialogue)
    client_text = " ".join(client_lines).lower()

    limited_dialogue_markers = [
        "я зайнятий",
        "я занята",
        "не можу говорити",
        "не можу зараз",
        "мені незручно",
        "немає часу говорити",
        "передзвоніть",
        "передзвони",
        "зараз не можу",
        "не до розмови",
    ]
    driving_markers = [
        "за кермом",
        "за рулем",
        "без телефону",
        "не можу взяти телефон",
        "не можу користуватись телефоном",
    ]

    has_limited_dialogue = has_any_marker(client_text, limited_dialogue_markers)
    has_driving_context = has_any_marker(client_text, driving_markers)

    features["is_limited_dialogue"] = has_limited_dialogue or has_driving_context
    features["client_driving_or_no_phone"] = has_driving_context
    return features


def validate_objection_and_retention(features, dialogue):
    manager_lines, client_lines = extract_role_lines(dialogue)
    manager_lines_lc = [line.lower() for line in manager_lines]
    client_lines_lc = [line.lower() for line in client_lines]
    manager_text = " ".join(manager_lines_lc)
    client_text = " ".join(client_lines_lc)

    end_call_markers = [
        "не можу говорити",
        "не можу зараз",
        "немає часу говорити",
        "я зайнятий",
        "я занята",
        "передзвоніть",
        "за кермом",
        "незручно говорити",
    ]
    product_objection_markers = [
        "не хочу грати",
        "не до гри",
        "не доіг",
        "не доігр",
        "не цікаво грати",
        "нецікаво грати",
        "не хочу бонус",
        "не хочу ніяких бонусів",
        "не граю",
        "не буду грати",
        "більше не граю",
        "кинув грати",
        "завязав з грою",
        "не займаюсь цим",
    ]
    real_retention_markers = [
        "буквально хвилин",
        "буквально секунд",
        "1 хвилин",
        "одну хвилин",
        "дуже коротко",
        "скажу головне",
        "лише головне",
        "одразу головне",
        "коротко поясню",
        "коротко розкажу",
    ]
    callback_only_markers = [
        "коли вам передзвонити",
        "на який час",
        "о котрій",
        "коли буде зручно",
    ]
    short_talk_markers = [
        "маєте пару хвилин",
        "є пару хвилин",
        "є хвилинка",
        "можна хвилинку",
        "можна 30 секунд",
        "є 30 секунд",
    ]
    objection_argument_markers = [
        "тому що",
        "бо ",
        "це дає",
        "вигід",
        "переваг",
        "чому це корисно",
        "сенс у тому",
        "дозвольте пояснити",
        "поясню коротко",
    ]

    def count_signal_lines(lines, markers):
        total = 0
        for line in lines:
            if any(marker in line for marker in markers):
                total += 1
        return total

    client_wants_to_end = has_any_marker(client_text, end_call_markers)
    product_objection = has_any_marker(client_text, product_objection_markers)
    end_signal_count = count_signal_lines(client_lines_lc, end_call_markers)
    product_objection_count = count_signal_lines(client_lines_lc, product_objection_markers)
    real_retention = has_any_marker(manager_text, real_retention_markers) or has_any_marker(manager_text, short_talk_markers)
    callback_only = has_any_marker(manager_text, callback_only_markers)
    manager_argumented = (
        has_any_marker(manager_text, real_retention_markers)
        or has_any_marker(manager_text, objection_argument_markers)
    )
    bonus_only = "бонус" in manager_text and not real_retention

    if client_wants_to_end:
        features["client_wants_to_end"] = True

    if client_wants_to_end and not product_objection:
        features["objection_detected"] = False

    if product_objection:
        features["objection_detected"] = True
        # Явне заперечення щодо гри ("не хочу грати", "не до гри" тощо)
        # означає намір завершити тему/розмову для критерію утримання.
        features["client_wants_to_end"] = True
        if features.get("continuation_level") == "none" and (real_retention or manager_argumented):
            features["continuation_level"] = "weak"

    if client_wants_to_end:
        if real_retention:
            if features.get("continuation_level") not in {"strong", "weak"}:
                features["continuation_level"] = "weak"
        elif callback_only or bonus_only:
            if features.get("continuation_level") == "strong":
                features["continuation_level"] = "formal"
            elif features.get("continuation_level") == "none":
                features["continuation_level"] = "formal"

    if features.get("continuation_level") == "strong":
        strong_count = 0
        strong_count += int(real_retention)
        strong_count += int("інший раз" in manager_text or "через годину" in manager_text or "ближче до вечора" in manager_text)
        if strong_count < 2:
            features["continuation_level"] = "weak"

    if features.get("objection_detected") and features.get("continuation_level") == "none":
        if features.get("client_hung_up_interrupted") or real_retention or manager_argumented:
            features["continuation_level"] = "weak"

    # Якщо клієнт озвучив заперечення по грі, але менеджер не зробив реальної
    # спроби втримання/аргументації, утримання має бути 0, а не "нейтрально".
    if product_objection and not real_retention and not manager_argumented:
        features["continuation_level"] = "none"
        features["continuation_behavior"] = "passive"

    # Якщо клієнт 2+ рази повторює заперечення і менеджер аргументує,
    # для "Робота із запереченнями" потрібен максимум (через lvl=strong у score_call).
    if product_objection_count >= 2 and manager_argumented:
        features["objection_detected"] = True
        if features.get("continuation_level") != "forced_end":
            features["continuation_level"] = "strong"

    # Якщо клієнт 2+ рази намагається завершити розмову,
    # максимум за "Утримання клієнта" можливий тільки коли була хоча б 1 реальна спроба втримання.
    if end_signal_count >= 2 and features.get("client_wants_to_end") and real_retention:
        if features.get("continuation_level") != "forced_end":
            features["continuation_level"] = "strong"

    # Якщо менеджер одразу після привітання додумав, що клієнту незручно,
    # і клієнт підтвердив незручність, але менеджер не зробив реальної спроби утримати —
    # утримання не зараховується (0 балів).
    # "Скорочений дзвінок" тут = відсутність реальної спроби утримання після сигналу клієнта,
    # а не обов'язково обрив. Якщо менеджер просто поїхав у презентацію, ігноруючи сигнал —
    # це провал утримання.
    if features.get("assumption_made"):
        client_confirmed_inconvenience_markers = [
            "відволікає",
            "відволікаєте",
            "відволікайте",
            "відриваєте",
            "заважаєте",
            "трошки відволік",
            "трохи відволік",
            "немає часу",
            "нема часу",
            "зайнят",
            "не можу говорити",
            "незручно",
            "не зручно",
            "не до розмови",
            "не до цього",
            "передзвоніть",
            "наберіть пізніше",
        ]
        client_confirmed = any(m in client_text for m in client_confirmed_inconvenience_markers)

        if client_confirmed and not real_retention:
            features["continuation_level"] = "none"
            # У score_call, коли client_wants_to_end=False, використовується
            # continuation_behavior. Примусово встановлюємо "passive", щоб у обох
            # гілках скорингу "Утримання клієнта" виходив 0.
            features["continuation_behavior"] = "passive"

    return features


def comment_mentions_military_service(comment):
    text = str(comment or "").lower()
    if not text.strip():
        return False

    military_markers = [
        "військов",
        "всу",
        "зсу",
        "служ",
        "на службі",
        "на службе",
        "военн",
        "арм",
        "мобіліз",
    ]
    negative_markers = [
        "не військов",
        "не военн",
        "не служ",
        "не в зсу",
        "не у зсу",
        "не в всу",
        "не у всу",
    ]

    if any(marker in text for marker in negative_markers):
        return False

    return any(marker in text for marker in military_markers)


def is_client_military(dialogue):
    if not dialogue:
        return False

    text = str(dialogue).lower()

    military_markers = [
        "військов",
        "всу",
        "зсу",
        "служ",
        "на службі",
        "на службе",
        "военн",
        "арм",
        "мобіліз",
    ]
    negative_markers = [
        "не військов",
        "не военн",
        "не служ",
        "не в зсу",
        "не у зсу",
        "не в всу",
        "не у всу",
    ]

    if any(marker in text for marker in negative_markers):
        return False

    return any(marker in text for marker in military_markers)


def validate_special_client_states(features, dialogue):
    manager_lines, client_lines = extract_role_lines(dialogue)
    client_text = " ".join(client_lines).lower()
    manager_text = " ".join(manager_lines).lower()
    all_text = (dialogue or "").lower()

    sick_markers = [
        "хворію", "хворий", "хвора", "не здужаю", "застудився",
        "застудилась", "температура", "погано себе почуваю", "нездужаю"
    ]
    if any(m in client_text for m in sick_markers):
        features["client_sick"] = True

    recovery_markers = [
        "одужуйте", "одужуй", "поправляйтесь", "поправляйся",
        "хай одужує", "бажаю одужання", "одужання"
    ]
    if any(m in manager_text for m in recovery_markers):
        features["manager_wished_recovery"] = True

    farewell_markers = [
        "до побачення", "до зустрічі", "всього доброго",
        "всього найкращого", "бажаю найкращого",
        "бувайте", "бувай", "на все добре",
        "щасливо", "до зв'язку",
        "на зв'язку", "будемо на зв'язку",
        "передзвоню", "наберу вас", "наберу пізніше",
        "гарного дня", "гарного вечора", "гарного тижня", "гарних вихідних",
        "приємного дня", "приємного вечора",
        "вдалого дня", "вдалого вечора",
        "бережіть себе", "бережи себе",
        "успіхів вам", "успіхів",
    ]
    if any(m in all_text for m in farewell_markers):
        features["has_farewell"] = True

    if is_client_military(dialogue):
        features["client_military"] = True

    return features


def build_combined_analysis_prompt(prompt_body, raw_dialogue, replacements):
    _ = replacements
    return f"""
{prompt_body}

ANALYSIS
---------------------

- аналізуй транскрипт як є (він уже очищений локально)
- поверни тільки `features`
- додатково визнач:
  "conversation_logically_completed" = true, якщо розмова по суті завершена
  "client_negative" = true, якщо клієнт проявляє негатив
  "client_used_profanity" = true, якщо клієнт використовує нецензурну лексику
  "manager_hung_up_before_client_finished" = true, якщо менеджер не дослухав клієнта і сам завершив незавершену розмову

Поверни ONLY valid JSON згідно зі схемою `ФОРМАТ JSON`, описаною вище.

СИРИЙ ТРАНСКРИПТ:
{raw_dialogue}
"""


def parse_analysis_response(text):
    match = re.search(r"\{[\s\S]*\}", text or "")
    if not match:
        return None

    payload = json.loads(match.group())
    features = apply_defaults(payload.get("features", {}))

    return {
        "features": features,
    }


def extract_features_openai(dialogue, comment, kb_context="", replacements=None):
    base_prompt = get_full_analysis_prompt_openai(comment, kb_context)
    prompt = build_combined_analysis_prompt(base_prompt, dialogue, replacements or {})

    max_output_tokens = OPENAI_MAX_OUTPUT_TOKENS
    last_error = None

    for _attempt in range(2):
        if _attempt > 0:
            st.warning(f"Retry attempt {_attempt}: невалідний JSON від моделі. Помилка: {last_error}")
        try:
            res = client.chat.completions.create(
                model=OPENAI_ANALYSIS_MODEL,
                temperature=0,
                max_completion_tokens=max_output_tokens,
                messages=[
                    {"role": "system", "content": "Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ]
            )
            parsed = parse_analysis_response(res.choices[0].message.content)
            if parsed:
                return parsed
            last_error = "empty or invalid JSON"
        except Exception as e:
            last_error = str(e)

        max_output_tokens = int(max_output_tokens * 1.6)

    st.error(f"GPT error: {last_error}")
    return {}


def extract_features_claude(dialogue, comment, kb_context="", replacements=None):
    if claude_client is None:
        st.error("Claude API key не налаштований (ANTHROPIC_API_KEY).")
        return {}

    base_prompt = get_full_analysis_prompt_claude(comment, kb_context)
    prompt = build_combined_analysis_prompt(base_prompt, dialogue, replacements or {})

    max_output_tokens = CLAUDE_MAX_OUTPUT_TOKENS
    last_error = None

    for _attempt in range(2):
        if _attempt > 0:
            st.warning(f"Retry attempt {_attempt}: невалідний JSON від моделі. Помилка: {last_error}")
        try:
            response = claude_client.messages.create(
                model=CLAUDE_ANALYSIS_MODEL,
                max_tokens=max_output_tokens,
                messages=[
                    {
                        "role": "user",
                        "content": f"Return ONLY valid JSON.\n{prompt}"
                    }
                ]
            )

            parsed = parse_analysis_response(response.content[0].text)
            if parsed:
                return parsed
            last_error = "empty or invalid JSON"
        except Exception as e:
            last_error = str(e)

        max_output_tokens = int(max_output_tokens * 1.6)

    st.error(f"Claude error: {last_error}")
    return {}


@st.cache_data(ttl=604800, show_spinner=False)
def analyze_call_cached(ai_provider, url, call_date, dialogue, manager_comment, kb_context, replacements, cache_version):
    if ai_provider == "openai":
        return extract_features_openai(
            dialogue,
            manager_comment,
            kb_context,
            replacements,
        )

    return extract_features_claude(
        dialogue,
        manager_comment,
        kb_context,
        replacements,
    )


def run_all_validators(features, dialogue, call, kb_data):
    """
    Єдина точка входу для всіх валідаторів.
    Порядок виклику критичний — не змінювати.
    """
    # 1. Презентація — залежить від KB, незалежна від інших
    features = normalize_presentation_level(features, dialogue, kb_data)

    # 2. Бонус — незалежний від презентації
    features = validate_bonus_features(features, dialogue)

    # 3. Діалог — визначає is_limited_dialogue і client_driving
    features = validate_dialogue_exceptions(features, dialogue)

    # 4. Додумування — перевіряє репліки менеджера і клієнта
    features = validate_assumption_made(features, dialogue)

    # 5. Заперечення і утримання — залежить від client_wants_to_end
    features = validate_objection_and_retention(features, dialogue)

    # 6. Причина в картці — читає коментар менеджера
    features = validate_card_reason(features, call["manager_comment"])

    # 7. Картка — залежить від followup_type і card_has_reason
    features = validate_card_features(features)

    # 8. Час у картці — читає коментар менеджера, після validate_card_features
    features = validate_card_followup_time(features, call["manager_comment"])

    # 9. Домовленість — уточнює followup_type по тексту
    features = validate_followup_type(features, dialogue)

    # 10. Картку повторно — бо followup_type міг змінитись на кроці 9
    features = validate_card_features(features)

    # 11. Професіоналізм — перевіряє third party
    features = validate_professionalism_features(features, dialogue)

    # 12. Заборонені слова — незалежний
    features = validate_forbidden_words(features, dialogue)

    # 13. Дружнє питання — уточнює friendly_question
    features = validate_friendly_question(features, dialogue)

    # 14. Спеціальні стани клієнта — has_farewell, client_sick, military
    features = validate_special_client_states(features, dialogue)

    return features


# ================= SCORING =================
def score_call(f, meta, dialogue=None):
    s = {}
    noise_reaction = f.get("noise_reaction", "none")
    followup_type = f.get("followup_type", "none")
    followup_attempts_count = int(f.get("followup_attempts_count") or 0)
    is_military_client = comment_mentions_military_service(meta.get("manager_comment", ""))
    is_driving_or_no_phone = bool(f.get("client_driving_or_no_phone"))
    unethical_client_behavior = bool(f.get("client_unethical_behavior"))
    manager_unethical_response = bool(f.get("manager_unethical_response"))

    # якщо автовідповідач → всі 0
    if dialogue and is_autoresponder(dialogue):
        return {
            "Встановлення контакту": 0,
            "Спроба презентації": 0,
            "Домовленість про наступний контакт": 0,
            "Пропозиція бонусу": 0,
            "Завершення розмови": 0,
            "Передзвон клієнту": 0,
            "Не додумувати": 0,
            "Якість мовлення": 0,
            "Професіоналізм": 0,
            "Оформлення картки": 0,
            "Утримання клієнта": 0,
            "Робота із запереченнями": 0
        }

    # Обмежений діалог (клієнт зайнятий/за кермом/просить передзвонити тощо):
    # за правилами промпта не занижуємо за відсутність презентації/аргументації
    # та не штрафуємо за відсутність утримання.
    limited_dialogue = bool(f.get("is_limited_dialogue"))

    # Особливий сценарій: клієнт сам перервав розмову під час заперечення.
    # За правилами А/Б: Бонус/Презентація/Домовленість — максимум; Передзвон: 15 тільки якщо був протягом години, інакше 0.
    objection_interrupted = (
        meta.get("call_completion_status") == "🟢 (слухавку поклав клієнт)"
        and bool(f.get("objection_detected"))
        and not bool(f.get("conversation_logically_completed"))
    )

    # ---------------- Контакт ----------------
    elements = sum([
        f["manager_name_present"],
        f["manager_position_present"],
        f["company_present"],
        f["client_name_used"],
        f["purpose_present"],
        f.get("friendly_question", False) or noise_reaction == "correct"
    ])

    contact_score = (
        7.5 if elements >= 4 else
        5 if elements == 3 else
        2.5 if elements == 2 else
        0
    )

    if not f.get("client_name_used"):
        contact_score -= 2.5

    if (
        (f.get("client_sick") and not f.get("manager_wished_recovery"))
        or (is_military_client and not f.get("manager_thanked_for_service"))
    ):
        contact_score -= 2.5

    s["Встановлення контакту"] = max(0, contact_score)

    # ---------------- Спроба презентації ----------------
    # Бінарна шкала: 0 або 5. partial і full дають однаковий максимум.
    level = f.get("presentation_level", "none")

    # limited_dialogue автоматично не зараховує презентацію, якщо менеджер явно
    # говорив про бонус (бонус ≠ презентація). Якщо менеджер у такій розмові
    # описував бонус — 0 балів, не пільговий максимум.
    manager_lines_for_score, _ = extract_role_lines(dialogue or "")
    manager_text_for_score = " ".join(manager_lines_for_score).lower()
    bonus_content_markers = [
        "бонус",
        "фс",
        "fs",
        "фріспін",
        "фриспін",
        "кешбек",
        "бездеп",
        "фрібет",
        "захист ставк",
        "від себе",
        "від менеджера",
        "без вейдж",
        "без відіграш",
        "обертів",
    ]
    manager_discussed_bonus = any(m in manager_text_for_score for m in bonus_content_markers)
    limited_dialogue_credit = limited_dialogue and not manager_discussed_bonus

    presentation_credited = (
        is_driving_or_no_phone
        or limited_dialogue_credit
        or level in {"full", "partial"}
    )
    s["Спроба презентації"] = 5 if presentation_credited else 0

    # ---------------- Домовленість ----------------
    # Без підтвердженої згоди клієнта на наступний контакт не даємо максимум:
    # 2+ спроби менеджера або обрив з боку клієнта = часткове виконання.
    has_partial_followup_signal = (
        followup_type == "offer"
        or followup_attempts_count >= 2
        or (
            meta.get("call_completion_status") == "🟢 (слухавку поклав клієнт)"
            and f.get("client_hung_up_interrupted")
        )
    )
    s["Домовленість про наступний контакт"] = (
        5 if followup_type == "exact_time"
        else 2.5 if has_partial_followup_signal
        else 0
    )

    # ---------------- Бонус ----------------
    # Якщо клієнт поклав слухавку під час етапу з бонусом, критерій бонусу зараховуємо на максимум.
    bonus_mentioned_in_dialogue = (
        "бонус" in (dialogue or "").lower()
        or "фріспін" in (dialogue or "").lower()
        or "фриспін" in (dialogue or "").lower()
        or "кешбек" in (dialogue or "").lower()
        or "фрібет" in (dialogue or "").lower()
    )

    client_hung_up_on_bonus_stage = (
        meta.get("call_completion_status") == "🟢 (слухавку поклав клієнт)"
        and not f.get("conversation_logically_completed")
        and (f.get("bonus_offered") or bonus_mentioned_in_dialogue)
    )
    if is_driving_or_no_phone:
        s["Пропозиція бонусу"] = 10
    elif client_hung_up_on_bonus_stage:
        s["Пропозиція бонусу"] = 10
    elif not f.get("bonus_offered"):
        s["Пропозиція бонусу"] = 0
    else:
        bonus_conditions = sum([
            bool(f.get("bonus_has_type")),
            bool(f.get("bonus_has_duration")),
            bool(f.get("bonus_has_value"))
        ])
        if bonus_conditions <= 0:
            s["Пропозиція бонусу"] = 0
        else:
            s["Пропозиція бонусу"] = 10 if bonus_conditions >= 2 else 5

    # ---------------- Завершення ----------------
    s["Завершення розмови"] = 5 if f.get("has_farewell") else 0

    # ---------------- Передзвон ----------------
    repeat = meta["repeat_call"]

    if followup_type == "none":
        s["Передзвон клієнту"] = 15
    elif followup_type == "offer":
        # Нечітка домовленість (тільки день без часу) — передзвон зараховується автоматично.
        s["Передзвон клієнту"] = 15
    elif objection_interrupted:
        s["Передзвон клієнту"] = (
            15 if repeat == "так, був протягом години"
            else 0
        )
    else:
        s["Передзвон клієнту"] = (
            15 if repeat == "так, був протягом години"
            else 10 if repeat == "так, був протягом 2 годин"
            else 0
        )

    # ---------------- Не додумувати ----------------
    if f.get("assumption_made"):
        s["Не додумувати"] = 2.5 if f.get("assumption_soft") else 0
    else:
        s["Не додумувати"] = 5

    # ---------------- Якість мовлення ----------------
    quality = f.get("speech_quality", "bad")

    if quality == "good":
        s["Якість мовлення"] = 2.5
    else:
        s["Якість мовлення"] = 0

    # ---------------- Професіоналізм ----------------
    if f.get("forbidden_words_used") or (
        f.get("client_not_actual_client") and f.get("manager_shared_bonus_with_third_party")
    ):
        s["Професіоналізм"] = 0
    else:
        s["Професіоналізм"] = (
            5 if meta["bonus_check"] == "помилково нараховано" else 10
        )

    # ---------------- Картка ----------------
    card_elements = sum([
        bool(f.get("card_has_reason")),
        bool(f.get("card_has_followup_time")),
    ])
    s["Оформлення картки"] = 5 if card_elements == 2 else 2.5 if card_elements == 1 else 0

    # ---------------- Утримання ----------------
    lvl = f.get("continuation_level", "none")

    if is_military_client:
        s["Утримання клієнта"] = 20
    elif limited_dialogue:
        s["Утримання клієнта"] = 20
    elif not f.get("client_wants_to_end"):
        behavior = f.get("continuation_behavior", "neutral")
        s["Утримання клієнта"] = (
            20 if behavior == "active"
            else 15 if behavior == "neutral"
            else 0 if behavior == "passive"
            else 0
        )
    else:
        s["Утримання клієнта"] = (
            20 if lvl == "strong"
            else 15 if lvl == "weak"
            else 10 if lvl == "formal"
            else 0
        )

    # ---------------- Заперечення ----------------
    if is_military_client:
        s["Робота із запереченнями"] = 10
    elif limited_dialogue:
        s["Робота із запереченнями"] = 10
    elif not f.get("objection_detected"):
        s["Робота із запереченнями"] = 10
    else:
        s["Робота із запереченнями"] = (
            10 if lvl == "strong"
            else 5 if lvl in {"weak", "formal"}
            else 0
        )

    if unethical_client_behavior and not manager_unethical_response:
        return {
            "Встановлення контакту": 7.5,
            "Спроба презентації": 5,
            "Домовленість про наступний контакт": 5,
            "Пропозиція бонусу": 10,
            "Завершення розмови": 5,
            "Передзвон клієнту": 15,
            "Не додумувати": 5,
            "Якість мовлення": 2.5,
            "Професіоналізм": 10,
            "Оформлення картки": 5,
            "Утримання клієнта": 20,
            "Робота із запереченнями": 10,
        }

    if objection_interrupted:
        s["Спроба презентації"] = 5
        s["Домовленість про наступний контакт"] = 5
        s["Пропозиція бонусу"] = 10

    return apply_call_completion_rules(s, f, meta)


def format_comment_for_sheet(comment):
    if not comment:
        return ""

    lines = [line.strip() for line in str(comment).splitlines() if line.strip()]
    return " | ".join(lines)


def build_readable_qa_comment(features, scores, call):
    lines = []

    contact_elements = sum([
        bool(features.get("manager_name_present")),
        bool(features.get("manager_position_present")),
        bool(features.get("company_present")),
        bool(features.get("client_name_used")),
        bool(features.get("purpose_present")),
    ])
    if scores.get("Встановлення контакту", 0) >= 5:
        lines.append("Встановлення контакту: менеджер коректно представився, звернувся до клієнта та озвучив мету дзвінка.")
    elif contact_elements >= 2:
        lines.append("Встановлення контакту: контакт встановлено частково, але не всі обов'язкові елементи були озвучені.")
    else:
        lines.append("Встановлення контакту: менеджер не представився повноцінно і не окреслив мету дзвінка.")

    presentation_level = features.get("presentation_level", "none")
    if features.get("client_driving_or_no_phone"):
        lines.append("Спроба презентації: клієнт не міг повноцінно взаємодіяти з телефоном, тому критерій зараховано за винятком.")
    elif presentation_level in {"full", "partial"}:
        lines.append("Спроба презентації: менеджер назвав продукт або активність і пояснив суть чи де знайти інформацію, тому презентацію зараховано повністю.")
    else:
        lines.append("Спроба презентації: презентації продукту не було; інформація лише про бонус не рахується як презентація.")

    followup_type = features.get("followup_type", "none")
    if features.get("client_hung_up_interrupted"):
        lines.append("Домовленість про наступний контакт: клієнт завершив дзвінок завчасно, тож за відсутності підтвердженої згоди критерій зараховано частково.")
    elif int(features.get("followup_attempts_count") or 0) >= 2:
        lines.append("Домовленість про наступний контакт: менеджер зробив щонайменше дві спроби домовитися, але без підтвердженої згоди клієнта критерій зараховано частково.")
    elif followup_type == "exact_time":
        lines.append("Домовленість про наступний контакт: узгоджено конкретний час наступного дзвінка.")
    elif followup_type == "offer":
        lines.append("Домовленість про наступний контакт: передзвон запропоновано, але без узгодженого точного часу.")
    else:
        lines.append("Домовленість про наступний контакт: домовленості про наступний дзвінок не було.")

    bonus_auto_due_client_hangup = (
        call.get("call_completion_status") == "🟢 (слухавку поклав клієнт)"
        and not features.get("conversation_logically_completed")
        and features.get("bonus_offered")
    )
    if not features.get("bonus_offered"):
        lines.append("Пропозиція бонусу: бонус клієнту не озвучено.")
    elif bonus_auto_due_client_hangup:
        lines.append("Пропозиція бонусу: клієнт завершив дзвінок на етапі бонусу, тому критерій зараховано на максимум за винятком.")
    else:
        bonus_details = []
        if features.get("bonus_has_type"):
            bonus_details.append("тип бонусу")
        if features.get("bonus_has_duration"):
            bonus_details.append("термін дії")
        if features.get("bonus_has_value"):
            bonus_details.append("розмір бонусу")
        if scores.get("Пропозиція бонусу", 0) >= 10:
            lines.append("Пропозиція бонусу: бонус озвучено як вигоду, названо щонайменше дві його умови.")
        else:
            detail_text = ", ".join(bonus_details) if bonus_details else "лише частину умов"
            lines.append(f"Пропозиція бонусу: бонус згадано формально, озвучено {detail_text}.")

    if features.get("has_farewell"):
        lines.append("Завершення розмови: розмову завершено з прощанням.")
    elif call.get("call_completion_status") == "🟢 (слухавку поклав клієнт)":
        lines.append("Завершення розмови: клієнт завершив дзвінок, тому критерій зараховано автоматично.")
    else:
        lines.append("Завершення розмови: прощання наприкінці розмови відсутнє.")

    repeat_call = call.get("repeat_call", "")
    if scores.get("Передзвон клієнту", 0) == 15:
        if repeat_call == "так, був протягом години":
            lines.append("Передзвон клієнту: передзвон виконано протягом години.")
        elif call.get("call_completion_status") == "🟢 (слухавку поклав клієнт)" and features.get("client_hung_up_interrupted"):
            lines.append("Передзвон клієнту: розмова обірвалась з боку клієнта, тому окремий штраф за передзвон не застосовується.")
        else:
            lines.append("Передзвон клієнту: штрафу немає, додатковий передзвон у цьому сценарії не був потрібний.")
    elif scores.get("Передзвон клієнту", 0) == 10:
        lines.append("Передзвон клієнту: передзвон був, але не одразу, а протягом двох годин.")
    else:
        lines.append("Передзвон клієнту: потрібного передзвону не було, тому критерій не виконано.")

    if features.get("assumption_made"):
        if features.get("assumption_soft"):
            lines.append("Не додумувати: менеджер м'яко припустив стан клієнта (напр., 'не відволікаю?'), критерій виконано частково.")
        else:
            lines.append("Не додумувати: менеджер припускав стан або намір клієнта без прямого підтвердження, тому критерій провалено.")
    else:
        lines.append("Не додумувати: менеджер не додумував зайвого і тримався фактів розмови.")

    if features.get("speech_quality") == "good":
        lines.append("Якість мовлення: мовлення достатньо чисте та зрозуміле для аналізу.")
    else:
        lines.append("Якість мовлення: у мовленні є проблеми, які заважають сприйняттю або точному аналізу.")

    detected = [
        str(item).strip()
        for item in features.get("forbidden_words_detected", [])
        if str(item).strip()
    ]
    if detected:
        lines.append(
            "Професіоналізм: 0 балів, менеджер використав заборонені слова/фрази: "
            f"{', '.join(detected)}."
        )
    elif call.get("bonus_check") == "помилково нараховано":
        lines.append("Професіоналізм: критерій знижено через помилково нарахований бонус.")
    else:
        lines.append("Професіоналізм: заборонених слів зі списку не виявлено.")

    card_elements = sum([
        bool(features.get("card_has_reason")),
        bool(features.get("card_has_followup_time")),
    ])
    if card_elements == 2:
        lines.append("Оформлення картки: у коментарі є причина незавершеної розмови та час наступного контакту.")
    elif card_elements == 1:
        lines.append("Оформлення картки: у коментарі є лише один з обов'язкових елементів: причина або час наступного контакту.")
    else:
        lines.append("Оформлення картки: у коментарі немає ані причини незавершеної розмови, ані часу наступного контакту.")

    if not features.get("objection_detected"):
        lines.append("Робота із запереченнями: заперечень від клієнта не було.")
    elif scores.get("Робота із запереченнями", 0) >= 10:
        lines.append("Робота із запереченнями: менеджер відпрацював заперечення аргументовано.")
    elif scores.get("Робота із запереченнями", 0) >= 5:
        lines.append("Робота із запереченнями: була спроба відпрацювати заперечення, але недостатньо глибока.")
    else:
        lines.append("Робота із запереченнями: заперечення не були відпрацьовані.")

    if features.get("client_wants_to_end"):
        continuation_level = features.get("continuation_level", "none")
        if scores.get("Утримання клієнта", 0) >= 20 or continuation_level == "strong":
            lines.append("Утримання клієнта: менеджер зробив кілька змістовних спроб втримати клієнта в розмові.")
        elif scores.get("Утримання клієнта", 0) >= 15 or continuation_level == "weak":
            lines.append("Утримання клієнта: була одна реальна спроба втримати клієнта в розмові.")
        elif scores.get("Утримання клієнта", 0) >= 10 or continuation_level == "formal":
            lines.append("Утримання клієнта: реакція менеджера була формальною, без реальної спроби втримати клієнта.")
        else:
            lines.append("Утримання клієнта: менеджер не втримував клієнта в розмові, коли це було потрібно.")
    else:
        continuation_behavior = features.get("continuation_behavior", "neutral")
        if scores.get("Утримання клієнта", 0) >= 20 or continuation_behavior == "active":
            lines.append("Утримання клієнта: менеджер активно вів діалог і не давав розмові згаснути.")
        elif scores.get("Утримання клієнта", 0) >= 15 or continuation_behavior == "neutral":
            lines.append("Утримання клієнта: менеджер підтримував розмову на нормальному рівні без провалів.")
        elif scores.get("Утримання клієнта", 0) >= 10 or continuation_behavior == "passive":
            lines.append("Утримання клієнта: розмову вели пасивно, без достатньої ініціативи з боку менеджера.")
        else:
            lines.append("Утримання клієнта: менеджер допустив втрату розмови або сам спровокував її завершення.")

    return "\n".join(lines)


def use_test_project_scores_sheet(call):
    project = str(call.get("project") or "").strip().upper()
    return project == "TEST"


def use_test_ret_manager_custom_layout(call):
    project = str(call.get("project") or "").strip().upper()
    manager = str(call.get("ret_manager") or "").strip().lower()
    supported_managers = {"бурий андрій", "жарікова анастасія"}
    return project in {"TEST", "ТЕСТ"} and manager in supported_managers


def get_manager_sheet_settings(call):
    if use_test_ret_manager_custom_layout(call):
        return {
            "worksheet_name": "Оцінки",
            "start_column": 4,
            "scores_start_row": 1,
            "criteria_start_row": 5,
            "log_start_row": 20,
        }

    if use_test_project_scores_sheet(call):
        return {
            "worksheet_name": "AI",
            "start_column": 1,
            "scores_start_row": 1,
            "criteria_start_row": 5,
            "log_start_row": 20,
        }

    return {
        "worksheet_name": "Оцінки",
        "start_column": 4,
        "scores_start_row": 88,
        "criteria_start_row": 93,
        "log_start_row": 110,
    }


def apply_call_completion_rules(scores, features, meta):
    status = meta.get("call_completion_status", "")
    immediate_repeat = meta.get("repeat_call") == "так, був протягом години"
    has_any_repeat = meta.get("repeat_call") in {
        "так, був протягом години",
        "так, був протягом 2 годин",
    }
    followup_type = features.get("followup_type", "none")
    requires_repeat_call = followup_type == "exact_time"
    logical_completion = bool(features.get("conversation_logically_completed"))
    has_farewell = bool(features.get("has_farewell"))
    bonus_offered = bool(features.get("bonus_offered"))
    has_followup = followup_type != "none"
    client_negative = bool(features.get("client_negative"))
    client_used_profanity = bool(features.get("client_used_profanity"))
    manager_hung_up_early = bool(features.get("manager_hung_up_before_client_finished"))
    interrupted_client_hangup = bool(features.get("client_hung_up_interrupted"))

    if logical_completion and has_farewell:
        if requires_repeat_call and not has_any_repeat:
            scores["Передзвон клієнту"] = 0
        return scores

    if status == "🟢 (слухавку поклав клієнт)":
        scores["Завершення розмови"] = 5
        if interrupted_client_hangup:
            if scores.get("Домовленість про наступний контакт", 0) > 2.5:
                scores["Домовленість про наступний контакт"] = 2.5
            if requires_repeat_call and not has_any_repeat:
                scores["Передзвон клієнту"] = 0

        if (
            not logical_completion
            and not has_farewell
            and bonus_offered
            and has_followup
            and immediate_repeat
        ):
            return scores

        if client_negative and not client_used_profanity:
            if requires_repeat_call and not immediate_repeat:
                scores["Передзвон клієнту"] = 0
            return scores

        if client_negative and client_used_profanity and not immediate_repeat:
            return scores

        if (
            not logical_completion
            and not has_farewell
            and not bonus_offered
            and not has_followup
            and not immediate_repeat
        ):
            scores["Утримання клієнта"] = 0
            return scores

    if status == "🔴 (слухавку поклав менеджер)":
        if manager_hung_up_early or client_negative:
            scores["Утримання клієнта"] = 0
            return scores

    if status == "🟡 (технічні проблеми, зв'язок обірвався)":
        if not logical_completion and not has_any_repeat:
            scores["Передзвон клієнту"] = 0
            return scores

    if requires_repeat_call and not has_any_repeat:
        scores["Передзвон клієнту"] = 0

    return scores

# ================= RUN =================
if "results" not in st.session_state:
    st.session_state["results"] = []

col1, col2 = st.columns(2)
run_openai = col1.button("🚀 OpenAI", type="primary")
run_claude = col2.button("🧠 Claude")

if run_openai or run_claude:
    st.session_state["results"].clear()

    google_client = None
    replacements = {}
    kb_data = []
    kb_context = ""

    try:
        google_client = connect_google()
        replacements, kb_data, kb_context = get_reference_data()
        
    except Exception as e:
        st.error(f"Google connect error: {e}")

    keyterms = tuple(build_keyterms(kb_data, managers_config))

    for i, call in enumerate(calls):
        if not call["url"]:
            continue

        with st.spinner(f"Аналіз дзвінка {i+1}..."):

            raw_transcript = transcribe_audio(call["url"], keyterms=keyterms)
            if not raw_transcript:
                st.warning("Немає транскрипції")
                continue

            transcript = clean_transcript_cached(raw_transcript, ANALYSIS_CACHE_VERSION)
            transcript = apply_replacements(transcript, replacements)

            analysis_result = analyze_call_cached(
                "openai" if run_openai else "claude",
                call["url"],
                call["call_date"],
                transcript,
                call["manager_comment"],
                kb_context,
                replacements,
                ANALYSIS_CACHE_VERSION,
            )

            if not analysis_result:
                st.warning("Помилка аналізу")
                continue

            clean_dialogue = apply_replacements(transcript, replacements)
            features = analysis_result.get("features", {})
            features = run_all_validators(features, clean_dialogue, call, kb_data)
            if not features:
                st.warning("Помилка аналізу")
                continue

            scores = score_call(features, call, clean_dialogue)
            comment = build_readable_qa_comment(features, scores, call)
            comment_for_sheet = format_comment_for_sheet(comment)
            ai_label = "OpenAI" if run_openai else "Claude"

            st.session_state["results"].append({
                "scores": scores,
                "comment": comment
            })

            if google_client:
                if not call["ret_sheet_id"]:
                    st.error("Не обрано проєкт або менеджера РЕТ")
                    continue

                total_score = sum(scores.values())
                sheet_settings = get_manager_sheet_settings(call)

                try:
                    workbook = google_client.open_by_key(call["ret_sheet_id"])
                    scores_sheet = (
                        workbook.worksheet(sheet_settings["worksheet_name"])
                        if sheet_settings["worksheet_name"]
                        else workbook.sheet1
                    )
                except Exception as e:
                    st.error(f"Google error [manager workbook]: {e}")
                    continue

                try:
                    res = write_to_google_sheet(
                        scores_sheet,
                        call,
                        scores,
                        start_column=sheet_settings["start_column"],
                        start_row=sheet_settings["scores_start_row"],
                        criteria_start_row=sheet_settings["criteria_start_row"],
                    )
                    st.write("WRITE RESULT:", res)
                    if res is not True:
                        st.error(f"Google error [scores write]: {res}")
                    else:
                        st.success(
                            f"Оцінки записано у таблицю менеджера `{call['ret_manager']}` "
                            f"(sheet id: {call['ret_sheet_id']}, аркуш: {sheet_settings['worksheet_name']})"
                        )
                except Exception as e:
                    st.error(f"Google error [scores write]: {e}")

                try:
                    manager_log_res = append_manager_log(
                        scores_sheet,
                        call,
                        comment_for_sheet,
                        total_score,
                        ai_label,
                        start_row=sheet_settings["log_start_row"],
                    )
                    if isinstance(manager_log_res, str):
                        st.error(f"Google error [manager log]: {manager_log_res}")
                except Exception as e:
                    st.error(f"Google error [manager log]: {e}")

                try:
                    log_workbook = google_client.open_by_key(LOG_SHEET_ID)
                except Exception as e:
                    st.error(f"Google error [QA logs workbook]: {e}")
                    log_workbook = None

                try:
                    if log_workbook is None:
                        raise RuntimeError("Не вдалося відкрити QA_LOGS_CALLS")
                    log_sheet = log_workbook.worksheet("Лист 1")
                    qa_log_res = append_qa_log(
                        log_sheet,
                        call,
                        transcript,
                        clean_dialogue,
                        comment,
                        total_score
                    )
                    if isinstance(qa_log_res, str):
                        st.error(f"Google error [QA log]: {qa_log_res}")
                except Exception as e:
                    st.error(f"Google error [QA log]: {e}")

                try:
                    if log_workbook is None:
                        raise RuntimeError("Не вдалося відкрити QA_LOGS_CALLS")
                    log_info_sheet = log_workbook.worksheet("LOG_INFO")
                    log_info_res = append_log_info(
                        log_info_sheet,
                        call,
                    )
                    if isinstance(log_info_res, str):
                        st.error(f"Google error [LOG_INFO]: {log_info_res}")
                except Exception as e:
                    st.error(f"Google error [LOG_INFO]: {e}")

# ================= OUTPUT =================
for i, res in enumerate(st.session_state["results"]):
    with st.expander(f"📞 Дзвінок {i+1}", expanded=(i == 0)):
        df = pd.DataFrame(
            list(res["scores"].items()),
            columns=["Критерій", "Оцінка"]
        )
        df["Оцінка"] = df["Оцінка"].apply(lambda x: f"{float(x):.1f}")
        st.table(df)

        total = sum(res["scores"].values())
        st.success(f"Загальний бал: {total:.1f}")

        st.markdown("### 💬 Коментар QA")
        for line in res["comment"].split("\n"):     
            st.write(line)

# ================= EXPORT =================
if st.session_state["results"]:
    xls = BytesIO()
    with pd.ExcelWriter(xls, engine="openpyxl") as writer:
        for i, res in enumerate(st.session_state["results"]):
            df = pd.DataFrame(res["scores"].items(), columns=["Критерій", "Оцінка"])
            df.to_excel(writer, sheet_name=f"Call_{i+1}", index=False)
    xls.seek(0)

    st.download_button(
        label="📥 Завантажити Excel",
        data=xls,
        file_name="qa_results.xlsx"
    )
