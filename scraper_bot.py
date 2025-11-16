import asyncio
import json
import logging
import os
import sys
from urllib.parse import urljoin  # <-- IMPORT ADDED

import httpx
from bs4 import BeautifulSoup
from telegram import Update
from telegram.error import Forbidden, BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    JobQueue,
)

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
OWNER_ID = os.environ.get("OWNER_ID")

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True) 

LINKS_FILE = os.path.join(DATA_DIR, "novels.json")
PROGRESS_FILE = os.path.join(DATA_DIR, "processed_pages.json")
LOG_FILE = os.path.join(DATA_DIR, "bot.log")

SAVE_BATCH_SIZE = 50
LAST_PAGE = 5076 
BASE_DOMAIN = "https://www.fanmtl.com" # <-- ADDED FOR JOINING RELATIVE URLS

# --- Logging ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


async def save_progress_async(links_set, pages_set):
    """Saves progress asynchronously."""
    logger.info("Saving progress to disk...")
    try:
        links_list = list(links_set)
        pages_list = list(pages_set)

        await asyncio.to_thread(
            json.dump, links_list, open(LINKS_FILE, "w"), indent=2
        )
        await asyncio.to_thread(
            json.dump, pages_list, open(PROGRESS_FILE, "w"), indent=2
        )
        logger.info(f"Progress saved. {len(links_list)} links, {len(pages_list)} pages.")
    except Exception as e:
        logger.error(f"Failed to save progress: {e}")


async def periodic_backup(context: ContextTypes.DEFAULT_TYPE):
    """Sends periodic backup to owner."""
    if not context.bot_data.get('is_scraping'):
        logger.info("Periodic backup: Scraping not active, skipping.")
        return

    logger.info("Periodic backup: Attempting to send links to owner...")
    try:
        await context.bot.send_document(
            chat_id=OWNER_ID, document=open(LINKS_FILE, "rb")
        )
        logger.info("Periodic backup successful.")
    except FileNotFoundError:
        logger.warning("Periodic backup: novels.json not found yet.")
    except Exception as e:
        logger.error(f"Periodic backup: Failed to send file: {e}")


async def scrape_all_novels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main background scraping task."""
    chat_id = update.effective_chat.id
    logger.info(f"Scraping task started for chat_id: {chat_id}")

    # --- 1. Load Existing State ---
    all_novel_links = set()
    processed_pages = set()

    try:
        with open(LINKS_FILE, "r") as f:
            all_novel_links = set(json.load(f))
        logger.info(f"Loaded {len(all_novel_links)} links from {LINKS_FILE}")
    except FileNotFoundError:
        logger.info(f"{LINKS_FILE} not found. Starting fresh.")
    except json.JSONDecodeError:
        logger.warning(f"{LINKS_FILE} is corrupt. Starting fresh.")
        all_novel_links = set()
    
    try:
        with open(PROGRESS_FILE, "r") as f:
            processed_pages = set(json.load(f))
        logger.info(f"Loaded {len(processed_pages)} processed pages from {PROGRESS_FILE}")
    except FileNotFoundError:
        logger.info(f"{PROGRESS_FILE} not found. Starting fresh.")
    except json.JSONDecodeError:
        logger.warning(f"{PROGRESS_FILE} is corrupt. Starting fresh.")
        processed_pages = set()


    # --- 2. Calculate Work ---
    total_pages_set = set(range(LAST_PAGE, -1, -1))
    pages_to_scrape = sorted(
        list(total_pages_set - processed_pages), reverse=True
    )
    
    if not pages_to_scrape:
        logger.info("No pages left to scrape. Exiting task.")
        await context.bot.send_message(chat_id=chat_id, text="✅ All pages are already processed.")
        context.bot_data['is_scraping'] = False
        return

    total_remaining = len(pages_to_scrape)
    logger.info(f"Resuming scrape. {total_remaining} pages left.")
    await context.bot.send_message(
        chat_id=chat_id, 
        text=f"Resuming scrape... {len(processed_pages)} pages already done.\n"
             f"{total_remaining} pages remaining."
    )

    # --- 3. Start Scraping Loop ---
    save_counter = 0
    base_url = "https://www.fanmtl.com/list/all/all-newstime-{}.html"

    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        
        for i, page_num in enumerate(pages_to_scrape):
            if not context.bot_data.get('is_scraping'):
                logger.info("Scraping cancelled by user.")
                await context.bot.send_message(chat_id=chat_id, text="Scraping manually stopped.")
                break
                
            try:
                url = base_url.format(page_num)
                r = await client.get(url)

                if r.status_code != 200:
                    logger.warning(f"Got {r.status_code} for {url}. Skipping.")
                    continue

                soup = BeautifulSoup(r.text, "html.parser")
                links_found_on_page = 0
                
                # --- START OF MODIFIED LOGIC ---
                for link_tag in soup.find_all("a", href=True):
                    href = link_tag['href']
                    
                    full_url = None
                    
                    # Case 1: Relative link (e.g., /novel/some-book)
                    if href.startswith("/novel/"):
                        full_url = urljoin(BASE_DOMAIN, href)
                        
                    # Case 2: Absolute link (e.g., https://www.fanmtl.com/novel/...)
                    elif href.startswith(f"{BASE_DOMAIN}/novel/"):
                        full_url = href

                    # If we found a valid URL, add it
                    if full_url:
                        if full_url not in all_novel_links:
                            all_novel_links.add(full_url)
                            links_found_on_page += 1
                # --- END OF MODIFIED LOGIC ---

                processed_pages.add(page_num)
                save_counter += 1

                if i % 100 == 0: 
                    logger.info(
                        f"[Progress {i}/{total_remaining}] Page {page_num}: "
                        f"Found {links_found_on_page} new links. "
                        f"Total: {len(all_novel_links)}"
                    )

                if save_counter >= SAVE_BATCH_SIZE:
                    await save_progress_async(all_novel_links, processed_pages)
                    save_counter = 0 

                await asyncio.sleep(0.1) 

            except httpx.ReadTimeout:
                logger.warning(f"Timeout on page {page_num}. Skipping.")
            except Exception as e:
                logger.error(f"Error on page {page_num}: {e}")

    # --- 4. Final Save & Cleanup ---
    logger.info("Scraping loop finished. Performing final save.")
    await save_progress_async(all_novel_links, processed_pages)
    context.bot_data['is_scraping'] = False
    
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"✅ **Scraping Complete!**\n\n"
             f"Processed {len(processed_pages)} total pages.\n"
             f"Found {len(all_novel_links)} unique novel links.\n"
             f"Use /get_all to download.",
        parse_mode="Markdown"
    )

# --- Bot Command Handlers (No changes below this line) ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends a welcome message."""
    await update.message.reply_text(
        "Hi! I'm your resumable scraper bot.\n\n"
        "**Commands:**\n"
        "• /scrape - Start/resume fetching all links.\n"
        "• /cancel - Stop the current scrape job.\n"
        "• /get_links - Download the links file.\n"
        "• /get_progress - Download the progress file.\n"
        "• /get_all - Download both files.",
        parse_mode="Markdown"
    )

async def start_scraping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Schedules the scraping task, if not already running."""
    if context.bot_data.get('is_scraping'):
        await update.message.reply_text("Scraping is already in progress.")
        return

    context.bot_data['is_scraping'] = True
    await update.message.reply_text("🚀 **Scraping job scheduled!**\n\nLoading state and starting...")
    
    asyncio.create_task(scrape_all_novels(update, context))

async def cancel_scraping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sets a flag to stop the scraping loop."""
    if not context.bot_data.get('is_scraping'):
        await update.message.reply_text("No scraping job is currently running.")
        return
    
    context.bot_data['is_scraping'] = False
    await update.message.reply_text("🛑 Sending stop signal... The scraper will halt after its current page.")


async def get_links_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends the links file (novels.json)."""
    try:
        await update.message.reply_document(document=open(LINKS_FILE, "rb"))
    except FileNotFoundError:
        await update.message.reply_text(f"{LINKS_FILE} not found. Run /scrape first.")
    except Exception as e:
        await update.message.reply_text(f"An error occurred: {e}")

async def get_progress_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends the progress file (processed_pages.json)."""
    try:
        await update.message.reply_document(document=open(PROGRESS_FILE, "rb"))
    except FileNotFoundError:
        await update.message.reply_text(f"{PROGRESS_FILE} not found. Run /scrape first.")
    except Exception as e:
        await update.message.reply_text(f"An error occurred: {e}")

async def get_all_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends both the links and progress files."""
    chat_id = update.effective_chat.id
    logger.info(f"User {chat_id} requested all files.")

    try:
        await context.bot.send_document(
            chat_id=chat_id, document=open(LINKS_FILE, "rb")
        )
    except FileNotFoundError:
        await context.bot.send_message(
            chat_id=chat_id, text=f"{LINKS_FILE} not found. Run /scrape first."
        )
    except Exception as e:
        await context.bot.send_message(
            chat_id=chat_id, text=f"An error occurred sending {LINKS_FILE}: {e}"
        )
        
    try:
        await context.bot.send_document(
            chat_id=chat_id, document=open(PROGRESS_FILE, "rb")
        )
    except FileNotFoundError:
        await context.bot.send_message(
            chat_id=chat_id, text=f"{PROGRESS_FILE} not found. Run /scrape first."
        )
    except Exception as e:
        await context.bot.send_message(
            chat_id=chat_id, text=f"An error occurred sending {PROGRESS_FILE}: {e}"
        )


def main():
    """Run the bot."""
    if not TELEGRAM_BOT_TOKEN:
        logger.critical("TELEGRAM_BOT_TOKEN environment variable not set. Exiting.")
        sys.exit(1)
    if not OWNER_ID:
        logger.critical("OWNER_ID environment variable not set. Exiting.")
        sys.exit(1)
    
    logger.info(f"Bot starting... Backups will be sent to OWNER_ID: {OWNER_ID}")

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    application.bot_data['is_scraping'] = False
    
    job_queue = application.job_queue
    job_queue.run_repeating(
        periodic_backup,
        interval=600,
        first=60
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("scrape", start_scraping_command))
    application.add_handler(CommandHandler("cancel", cancel_scraping_command))
    application.add_handler(CommandHandler("get_links", get_links_command))
    application.add_handler(CommandHandler("get_progress", get_progress_command))
    application.add_handler(CommandHandler("get_all", get_all_files))

    logger.info("Bot is starting to poll...")
    application.run_polling()


if __name__ == "__main__":
    main()
