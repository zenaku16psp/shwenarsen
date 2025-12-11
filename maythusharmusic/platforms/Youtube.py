# youtube.py
import asyncio
import os
import re
import json
import glob
from typing import Union
import yt_dlp
from pyrogram.enums import MessageEntityType
from pyrogram.types import Message
from youtubesearchpython.__future__ import VideosSearch
from maythusharmusic.utils.database import is_on_off
from maythusharmusic import app
from maythusharmusic.utils.formatters import time_to_seconds
import random
import logging
import aiohttp
import traceback
from maythusharmusic import LOGGER

# API Configuration
API_URL = "https://teaminflex.xyz"
API_KEY = "INFLEX68381428D"

# ==============================================
# 🎵 AUDIO DOWNLOAD
# ==============================================
async def download_song(link: str) -> str:
    """Download audio from YouTube using API"""
    # Extract video ID from link
    if 'v=' in link:
        video_id = link.split('v=')[-1].split('&')[0]
    elif 'youtu.be/' in link:
        video_id = link.split('youtu.be/')[-1].split('?')[0]
    else:
        video_id = link
    
    logger = LOGGER("InflexMusic/platforms/Youtube.py")
    logger.info(f"🎵 [AUDIO] Starting download process for ID: {video_id}")

    if not video_id or len(video_id) < 3:
        logger.error(f"Invalid video ID: {video_id}")
        return None

    DOWNLOAD_DIR = "downloads"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    # Try multiple possible file extensions
    possible_extensions = ['mp3', 'webm', 'm4a', 'opus']
    for ext in possible_extensions:
        file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.{ext}")
        if os.path.exists(file_path):
            logger.info(f"🎵 [LOCAL] Found existing {ext} file for ID: {video_id}")
            return file_path

    try:
        async with aiohttp.ClientSession() as session:
            payload = {"url": video_id, "type": "audio"}
            headers = {
                "Content-Type": "application/json",
                "X-API-KEY": API_KEY
            }

            # 🔹 Step 1: Trigger API download
            logger.info(f"[API] Requesting audio download for ID: {video_id}")
            async with session.post(f"{API_URL}/download", json=payload, headers=headers) as response:
                if response.status == 401:
                    logger.error("[API] Invalid API key")
                    return None
                if response.status != 200:
                    logger.error(f"[AUDIO] API returned {response.status}")
                    return None

                data = await response.json()
                if data.get("status") != "success" or not data.get("download_url"):
                    logger.error(f"[AUDIO] API response error: {data}")
                    return None

                download_link = f"{API_URL}{data['download_url']}"
                logger.info(f"[API] Got download link: {download_link}")

            # 🔹 Step 2: Download the file
            logger.info(f"[API] Downloading file from: {download_link}")
            async with session.get(download_link) as file_response:
                if file_response.status != 200:
                    logger.error(f"[AUDIO] Download failed ({file_response.status}) for ID: {video_id}")
                    return None
                
                # Get filename from Content-Disposition or use default
                content_disposition = file_response.headers.get('Content-Disposition', '')
                if 'filename=' in content_disposition:
                    original_filename = content_disposition.split('filename=')[-1].strip('"')
                    original_ext = original_filename.split('.')[-1].lower()
                    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.{original_ext}")
                else:
                    # Use webm as default for audio
                    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.webm")
                
                # Download file
                total_size = 0
                with open(file_path, "wb") as f:
                    async for chunk in file_response.content.iter_chunked(8192):
                        f.write(chunk)
                        total_size += len(chunk)
                
                logger.info(f"[API] Downloaded {total_size} bytes to {file_path}")

        # 🔹 Convert to mp3 if needed
        if not file_path.endswith('.mp3'):
            mp3_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.mp3")
            try:
                logger.info(f"[CONVERT] Converting {file_path} to mp3")
                cmd = [
                    'ffmpeg',
                    '-i', file_path,
                    '-codec:a', 'libmp3lame',
                    '-qscale:a', '2',
                    '-y',
                    mp3_path
                ]
                
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                
                if proc.returncode == 0 and os.path.exists(mp3_path):
                    file_size = os.path.getsize(mp3_path)
                    if file_size > 10240:  # At least 10KB
                        logger.info(f"[CONVERT] Successfully converted to mp3 ({file_size} bytes)")
                        # Remove original file
                        try:
                            os.remove(file_path)
                        except:
                            pass
                        file_path = mp3_path
                    else:
                        logger.warning(f"[CONVERT] Converted file too small ({file_size} bytes), keeping original")
                else:
                    logger.warning(f"[CONVERT] FFmpeg conversion failed, keeping original format")
                    if stdout:
                        logger.debug(f"FFmpeg stdout: {stdout.decode()[:200]}")
                    if stderr:
                        logger.debug(f"FFmpeg stderr: {stderr.decode()[:200]}")
                        
            except Exception as conv_e:
                logger.error(f"[CONVERT] Conversion error: {conv_e}")

        # Verify the file exists and has content
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            if file_size > 0:
                logger.info(f"🎵 [API] Download completed successfully for ID: {video_id} ({file_size} bytes)")
                return file_path
            else:
                logger.error(f"[API] Downloaded file is empty: {file_path}")
                try:
                    os.remove(file_path)
                except:
                    pass
                return None
        else:
            logger.error(f"[API] File was not created: {file_path}")
            return None

    except asyncio.TimeoutError:
        logger.error(f"[AUDIO] Timeout downloading ID: {video_id}")
        return None
    except aiohttp.ClientError as e:
        logger.error(f"[AUDIO] Network error for ID: {video_id} - {e}")
        return None
    except Exception as e:
        logger.error(f"[AUDIO] Exception for ID: {video_id} - {e}")
        traceback.print_exc()
        return None


# ==============================================
# 🎥 VIDEO DOWNLOAD
# ==============================================
async def download_video(link: str) -> str:
    """Download video from YouTube using API"""
    # Extract video ID from link
    if 'v=' in link:
        video_id = link.split('v=')[-1].split('&')[0]
    elif 'youtu.be/' in link:
        video_id = link.split('youtu.be/')[-1].split('?')[0]
    else:
        video_id = link
    
    logger = LOGGER("InflexMusic/platforms/Youtube.py")
    logger.info(f"🎥 [VIDEO] Starting download process for ID: {video_id}")

    if not video_id or len(video_id) < 3:
        logger.error(f"Invalid video ID: {video_id}")
        return None

    DOWNLOAD_DIR = "downloads"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    # Try multiple possible file extensions
    possible_extensions = ['mp4', 'mkv', 'webm']
    for ext in possible_extensions:
        file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.{ext}")
        if os.path.exists(file_path):
            logger.info(f"🎥 [LOCAL] Found existing {ext} file for ID: {video_id}")
            return file_path

    try:
        async with aiohttp.ClientSession() as session:
            payload = {"url": video_id, "type": "video"}
            headers = {
                "Content-Type": "application/json",
                "X-API-KEY": API_KEY
            }

            # 🔹 Step 1: Trigger API download
            logger.info(f"[API] Requesting video download for ID: {video_id}")
            async with session.post(f"{API_URL}/download", json=payload, headers=headers) as response:
                if response.status == 401:
                    logger.error("[API] Invalid API key")
                    return None
                if response.status != 200:
                    logger.error(f"[VIDEO] API returned {response.status}")
                    return None

                data = await response.json()
                if data.get("status") != "success" or not data.get("download_url"):
                    logger.error(f"[VIDEO] API response error: {data}")
                    return None

                download_link = f"{API_URL}{data['download_url']}"
                logger.info(f"[API] Got download link: {download_link}")

            # 🔹 Step 2: Download the file
            logger.info(f"[API] Downloading file from: {download_link}")
            async with session.get(download_link) as file_response:
                if file_response.status != 200:
                    logger.error(f"[VIDEO] Download failed ({file_response.status}) for ID: {video_id}")
                    return None
                
                # Get filename from Content-Disposition or use default
                content_disposition = file_response.headers.get('Content-Disposition', '')
                if 'filename=' in content_disposition:
                    original_filename = content_disposition.split('filename=')[-1].strip('"')
                    original_ext = original_filename.split('.')[-1].lower()
                    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.{original_ext}")
                else:
                    # Use mkv as default for video
                    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.mkv")
                
                # Download file
                total_size = 0
                with open(file_path, "wb") as f:
                    async for chunk in file_response.content.iter_chunked(8192):
                        f.write(chunk)
                        total_size += len(chunk)
                
                logger.info(f"[API] Downloaded {total_size} bytes to {file_path}")

        # 🔹 Convert to mp4 if needed (for better compatibility)
        if not file_path.endswith('.mp4'):
            mp4_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.mp4")
            try:
                logger.info(f"[CONVERT] Converting {file_path} to mp4")
                cmd = [
                    'ffmpeg',
                    '-i', file_path,
                    '-c:v', 'copy',
                    '-c:a', 'copy',
                    '-y',
                    mp4_path
                ]
                
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                
                if proc.returncode == 0 and os.path.exists(mp4_path):
                    file_size = os.path.getsize(mp4_path)
                    if file_size > 102400:  # At least 100KB
                        logger.info(f"[CONVERT] Successfully converted to mp4 ({file_size} bytes)")
                        # Remove original file
                        try:
                            os.remove(file_path)
                        except:
                            pass
                        file_path = mp4_path
                    else:
                        logger.warning(f"[CONVERT] Converted file too small ({file_size} bytes), keeping original")
                else:
                    logger.warning(f"[CONVERT] FFmpeg conversion failed, keeping original format")
                    
            except Exception as conv_e:
                logger.error(f"[CONVERT] Video conversion error: {conv_e}")

        # Verify the file exists and has content
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            if file_size > 0:
                logger.info(f"🎥 [API] Download completed successfully for ID: {video_id} ({file_size} bytes)")
                return file_path
            else:
                logger.error(f"[API] Downloaded file is empty: {file_path}")
                try:
                    os.remove(file_path)
                except:
                    pass
                return None
        else:
            logger.error(f"[API] File was not created: {file_path}")
            return None

    except asyncio.TimeoutError:
        logger.error(f"[VIDEO] Timeout downloading ID: {video_id}")
        return None
    except aiohttp.ClientError as e:
        logger.error(f"[VIDEO] Network error for ID: {video_id} - {e}")
        return None
    except Exception as e:
        logger.error(f"[VIDEO] Exception for ID: {video_id} - {e}")
        traceback.print_exc()
        return None


async def check_file_size(link):
    """Check file size of YouTube video"""
    async def get_format_info(link):
        cookie_file = "maythusharmusic/cookies/cookies.txt"
        if not cookie_file or not os.path.exists(cookie_file):
            logger = LOGGER("InflexMusic/platforms/Youtube.py")
            logger.warning("No cookies found or path is incorrect. Cannot check file size.")
            return None
            
        try:
            proc = await asyncio.create_subprocess_exec(
                "yt-dlp",
                "--cookies", cookie_file,
                "-J",
                link,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                logger.error(f'Error checking file size:\n{stderr.decode()}')
                return None
            return json.loads(stdout.decode())
        except Exception as e:
            logger.error(f"Error in get_format_info: {e}")
            return None

    def parse_size(formats):
        total_size = 0
        for format in formats:
            if 'filesize' in format:
                total_size += format['filesize']
        return total_size

    info = await get_format_info(link)
    if info is None:
        return None
    
    formats = info.get('formats', [])
    if not formats:
        logger = LOGGER("InflexMusic/platforms/Youtube.py")
        logger.warning("No formats found for file size check.")
        return None
    
    total_size = parse_size(formats)
    return total_size


async def shell_cmd(cmd):
    """Execute shell command"""
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, errorz = await proc.communicate()
        if errorz:
            error_str = errorz.decode("utf-8")
            if "unavailable videos are hidden" in error_str.lower():
                return out.decode("utf-8")
            else:
                return error_str
        return out.decode("utf-8")
    except Exception as e:
        logger = LOGGER("InflexMusic/platforms/Youtube.py")
        logger.error(f"Shell command error: {e}")
        return str(e)


class YouTubeAPI:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.regex = r"(?:youtube\.com|youtu\.be)"
        self.status = "https://www.youtube.com/oembed?url="
        self.listbase = "https://youtube.com/playlist?list="
        self.reg = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        self.logger = LOGGER("InflexMusic/platforms/Youtube.py")

    async def exists(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        return bool(re.search(self.regex, link))

    async def url(self, message_1: Message) -> Union[str, None]:
        messages = [message_1]
        if message_1.reply_to_message:
            messages.append(message_1.reply_to_message)
        for message in messages:
            if message.entities:
                for entity in message.entities:
                    if entity.type == MessageEntityType.URL:
                        text = message.text or message.caption
                        return text[entity.offset: entity.offset + entity.length]
            elif message.caption_entities:
                for entity in message.caption_entities:
                    if entity.type == MessageEntityType.TEXT_LINK:
                        return entity.url
        return None

    async def details(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        try:
            results = VideosSearch(link, limit=1)
            search_results = await results.next()
            if not search_results or "result" not in search_results or not search_results["result"]:
                return None, "0:00", 0, None, None
            
            result = search_results["result"][0]
            title = result.get("title", "Unknown Title")
            duration_min = result.get("duration", "0:00")
            thumbnail = result.get("thumbnails", [{}])[0].get("url", "").split("?")[0]
            vidid = result.get("id", "")
            duration_sec = int(time_to_seconds(duration_min)) if duration_min else 0
            
            return title, duration_min, duration_sec, thumbnail, vidid
        except Exception as e:
            self.logger.error(f"Error getting details: {e}")
            return None, "0:00", 0, None, None

    async def title(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        try:
            results = VideosSearch(link, limit=1)
            search_results = await results.next()
            if search_results and "result" in search_results and search_results["result"]:
                return search_results["result"][0].get("title", "Unknown Title")
        except Exception as e:
            self.logger.error(f"Error getting title: {e}")
        return "Unknown Title"

    async def duration(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        try:
            results = VideosSearch(link, limit=1)
            search_results = await results.next()
            if search_results and "result" in search_results and search_results["result"]:
                return search_results["result"][0].get("duration", "0:00")
        except Exception as e:
            self.logger.error(f"Error getting duration: {e}")
        return "0:00"

    async def thumbnail(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        try:
            results = VideosSearch(link, limit=1)
            search_results = await results.next()
            if search_results and "result" in search_results and search_results["result"]:
                thumbnails = search_results["result"][0].get("thumbnails", [])
                if thumbnails:
                    return thumbnails[0].get("url", "").split("?")[0]
        except Exception as e:
            self.logger.error(f"Error getting thumbnail: {e}")
        return None

    async def video(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        try:
            downloaded_file = await download_video(link)
            if downloaded_file:
                return 1, downloaded_file
            else:
                return 0, "Video download failed."
        except Exception as e:
            self.logger.error(f"Video download failed: {e}")
            return 0, f"Video download failed: {e}"

    async def playlist(self, link, limit, user_id, videoid: Union[bool, str] = None):
        if videoid:
            link = self.listbase + link
        if "&" in link:
            link = link.split("&")[0]
        
        cookie_file = "maythusharmusic/cookies/cookies.txt"
        if not cookie_file or not os.path.exists(cookie_file):
            self.logger.warning("cookies.txt not found for playlist extraction")
            return []
        
        try:
            playlist = await shell_cmd(
                f"yt-dlp -i --get-id --flat-playlist --cookies {cookie_file} --playlist-end {limit} --skip-download {link}"
            )
            result = [key for key in playlist.split("\n") if key.strip()]
            return result
        except Exception as e:
            self.logger.error(f"Error getting playlist: {e}")
            return []

    async def track(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        try:
            results = VideosSearch(link, limit=1)
            search_results = await results.next()
            if not search_results or "result" not in search_results or not search_results["result"]:
                return {}, ""
            
            result = search_results["result"][0]
            title = result.get("title", "Unknown Title")
            duration_min = result.get("duration", "0:00")
            vidid = result.get("id", "")
            yturl = result.get("link", link)
            thumbnail = result.get("thumbnails", [{}])[0].get("url", "").split("?")[0]
            
            track_details = {
                "title": title,
                "link": yturl,
                "vidid": vidid,
                "duration_min": duration_min,
                "thumb": thumbnail,
            }
            return track_details, vidid
        except Exception as e:
            self.logger.error(f"Error getting track details: {e}")
            return {}, ""

    async def formats(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        
        cookie_file = "maythusharmusic/cookies/cookies.txt"
        if not cookie_file or not os.path.exists(cookie_file):
            self.logger.warning("cookies.txt not found for formats extraction")
            return [], link
        
        try:
            ytdl_opts = {"quiet": True, "cookiefile": cookie_file}
            ydl = yt_dlp.YoutubeDL(ytdl_opts)
            with ydl:
                formats_available = []
                r = ydl.extract_info(link, download=False)
                for format in r.get("formats", []):
                    try:
                        if "dash" not in str(format.get("format", "")).lower():
                            formats_available.append({
                                "format": format.get("format", ""),
                                "filesize": format.get("filesize"),
                                "format_id": format.get("format_id", ""),
                                "ext": format.get("ext", ""),
                                "format_note": format.get("format_note", ""),
                                "yturl": link,
                            })
                    except:
                        continue
            return formats_available, link
        except Exception as e:
            self.logger.error(f"Error getting formats: {e}")
            return [], link

    async def slider(self, link: str, query_type: int, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        try:
            a = VideosSearch(link, limit=10)
            result = (await a.next()).get("result", [])
            if not result or query_type >= len(result):
                return "Unknown Title", "0:00", None, ""
            
            title = result[query_type].get("title", "Unknown Title")
            duration_min = result[query_type].get("duration", "0:00")
            vidid = result[query_type].get("id", "")
            thumbnail = result[query_type].get("thumbnails", [{}])[0].get("url", "").split("?")[0]
            return title, duration_min, thumbnail, vidid
        except Exception as e:
            self.logger.error(f"Error getting slider info: {e}")
            return "Unknown Title", "0:00", None, ""

    async def download(
        self,
        link: str,
        mystic,  # Pyrogram Message
        video: Union[bool, str] = None,
        videoid: Union[bool, str] = None,
        songaudio: Union[bool, str] = None,
        songvideo: Union[bool, str] = None,
        format_id: Union[bool, str] = None,
        title: Union[bool, str] = None,
    ) -> tuple:
        """
        Main download function with fallback mechanism
        Returns: (file_path, success)
        """
        
        if videoid:
            link = self.base + link
        
        self.logger.info(f"[Downloader] Starting download for: {link}")
        
        # Clean title for filename
        if title:
            # Remove invalid characters for filename
            title = re.sub(r'[<>:"/\\|?*]', '', title)
            title = title.strip().replace(" ", "_")[:50]
            if not title:  # If title becomes empty after cleaning
                title = None
        
        # Extract video ID
        if 'v=' in link:
            video_id = link.split('v=')[-1].split('&')[0]
        elif 'youtu.be/' in link:
            video_id = link.split('youtu.be/')[-1].split('?')[0]
        else:
            video_id = link
        
        file_name_base = title if title else video_id
        
        DOWNLOAD_DIR = "downloads"
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)

        # ================================
        # 1. TRY API FIRST
        # ================================
        try:
            # Update status message if available
            if mystic and hasattr(mystic, "edit_text"):
                try:
                    await mystic.edit_text("🎵 Downloading via API...")
                except:
                    pass
            
            # Determine download type
            is_video = video or songvideo
            
            if is_video:
                self.logger.info(f"[Downloader] Trying API video download for: {link}")
                downloaded_file = await download_video(link)
            else:
                self.logger.info(f"[Downloader] Trying API audio download for: {link}")
                downloaded_file = await download_song(link)
            
            if downloaded_file and os.path.exists(downloaded_file):
                file_size = os.path.getsize(downloaded_file)
                if file_size > 0:
                    self.logger.info(f"[Downloader] API download successful: {downloaded_file} ({file_size} bytes)")
                    
                    # Rename file with title if available
                    if title and downloaded_file:
                        try:
                            ext = os.path.splitext(downloaded_file)[1]
                            new_path = os.path.join(DOWNLOAD_DIR, f"{file_name_base}{ext}")
                            os.rename(downloaded_file, new_path)
                            downloaded_file = new_path
                            self.logger.info(f"[Downloader] Renamed to: {downloaded_file}")
                        except Exception as rename_e:
                            self.logger.warning(f"[Downloader] Could not rename file: {rename_e}")
                    
                    return downloaded_file, True
                else:
                    self.logger.error(f"[Downloader] API downloaded file is empty: {downloaded_file}")
                    try:
                        os.remove(downloaded_file)
                    except:
                        pass
            
            self.logger.warning(f"[Downloader] API download failed for {link}")
            
        except Exception as e:
            self.logger.error(f"[Downloader] API download exception: {e}")
            traceback.print_exc()

        # ================================
        # 2. FALLBACK TO YT-DLP
        # ================================
        try:
            # Update status message
            if mystic and hasattr(mystic, "edit_text"):
                try:
                    await mystic.edit_text("🔄 API failed, trying backup downloader...")
                except:
                    pass
            
            self.logger.warning(f"[Downloader] Falling back to yt-dlp for {link}")
            
            cookie_file = "maythusharmusic/cookies/cookies.txt"
            if not os.path.exists(cookie_file):
                self.logger.warning("[Downloader] cookies.txt not found, using yt-dlp without cookies")
                cookie_file = None

            # yt-dlp options
            ydl_opts = {
                "nocheckcertificate": True,
                "outtmpl": os.path.join(DOWNLOAD_DIR, f"{file_name_base}.%(ext)s"),
                "geo_bypass": True,
                "quiet": True,
                "retries": 3,
                "fragment_retries": 3,
                "no_warnings": False,
                "ignoreerrors": False,
                "noprogress": True,
            }

            if cookie_file:
                ydl_opts["cookiefile"] = cookie_file
            
            # Determine format based on request
            is_video = video or songvideo
            
            if is_video:
                # Video download
                if format_id:
                    ydl_opts["format"] = f"{format_id}+bestaudio"
                else:
                    ydl_opts["format"] = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best"
                
                ydl_opts["merge_output_format"] = "mp4"
                expected_ext = "mp4"
                expected_file = os.path.join(DOWNLOAD_DIR, f"{file_name_base}.mp4")
            else:
                # Audio download
                if format_id:
                    ydl_opts["format"] = format_id
                else:
                    ydl_opts["format"] = "bestaudio/best"
                
                ydl_opts["postprocessors"] = [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "320",
                }]
                expected_ext = "mp3"
                expected_file = os.path.join(DOWNLOAD_DIR, f"{file_name_base}.mp3")

            # Download with yt-dlp
            try:
                self.logger.info(f"[Downloader] Starting yt-dlp download with format: {ydl_opts.get('format')}")
                
                # Run yt-dlp in executor to avoid blocking
                loop = asyncio.get_event_loop()
                ydl = yt_dlp.YoutubeDL(ydl_opts)
                
                # First get info
                info = await loop.run_in_executor(
                    None, lambda: ydl.extract_info(link, download=False)
                )
                
                if not info:
                    self.logger.error("[Downloader] Failed to get video info")
                    return None, False
                
                # Then download
                await loop.run_in_executor(
                    None, lambda: ydl.download([link])
                )
                
                # Check for downloaded file
                if os.path.exists(expected_file):
                    file_size = os.path.getsize(expected_file)
                    if file_size > 0:
                        self.logger.info(f"[Downloader] yt-dlp download successful: {expected_file} ({file_size} bytes)")
                        return expected_file, True
                
                # Search for any matching file with different extension
                pattern = os.path.join(DOWNLOAD_DIR, f"{file_name_base}.*")
                files = glob.glob(pattern)
                valid_files = [
                    f for f in files 
                    if not f.endswith((".part", ".ytdl", ".temp")) 
                    and os.path.getsize(f) > 0
                ]
                
                if valid_files:
                    found_file = valid_files[0]
                    self.logger.info(f"[Downloader] Found valid file: {found_file}")
                    return found_file, True
                
                # Try to find by video ID
                if not title:  # If we were using title as base, also try video_id
                    pattern = os.path.join(DOWNLOAD_DIR, f"{video_id}.*")
                    files = glob.glob(pattern)
                    valid_files = [
                        f for f in files 
                        if not f.endswith((".part", ".ytdl", ".temp")) 
                        and os.path.getsize(f) > 0
                    ]
                    
                    if valid_files:
                        found_file = valid_files[0]
                        self.logger.info(f"[Downloader] Found file by video ID: {found_file}")
                        return found_file, True
                
                self.logger.error(f"[Downloader] No valid file found for {file_name_base}")
                return None, False
                
            except yt_dlp.utils.DownloadError as e:
                self.logger.error(f"[Downloader] yt-dlp DownloadError: {e}")
                return None, False
            except Exception as e:
                self.logger.error(f"[Downloader] yt-dlp exception: {e}")
                traceback.print_exc()
                return None, False
                
        except Exception as e:
            self.logger.error(f"[Downloader] Fallback exception: {e}")
            traceback.print_exc()
            return None, False
