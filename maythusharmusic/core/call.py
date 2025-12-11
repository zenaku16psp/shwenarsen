import asyncio
import os
import sys
import traceback
from datetime import datetime, timedelta
from typing import Union

from ntgcalls import TelegramServerError
from pyrogram import Client
from pyrogram.errors import FloodWait, ChatAdminRequired
from pyrogram.types import InlineKeyboardMarkup
from pytgcalls import PyTgCalls
from pytgcalls.exceptions import NoActiveGroupCall
from pytgcalls.types import AudioQuality, ChatUpdate, MediaStream, StreamEnded, Update, VideoQuality

import config
from strings import get_string
from maythusharmusic import LOGGER, YouTube, app
from maythusharmusic.misc import db
from maythusharmusic.utils.database import (
    add_active_chat,
    add_active_video_chat,
    get_lang,
    get_loop,
    group_assistant,
    is_autoend,
    music_on,
    remove_active_chat,
    remove_active_video_chat,
    set_loop,
)
from maythusharmusic.utils.exceptions import AssistantErr
from maythusharmusic.utils.formatters import check_duration, seconds_to_min, speed_converter
from maythusharmusic.utils.inline.play import stream_markup
from maythusharmusic.utils.stream.autoclear import auto_clean
from maythusharmusic.utils.thumbnails import get_thumb
from maythusharmusic.utils.errors import capture_internal_err, send_large_error

autoend = {}
counter = {}

def dynamic_media_stream(path: str, video: bool = False, ffmpeg_params: str = None) -> MediaStream:
    return MediaStream(
        audio_path=path,
        media_path=path,
        audio_parameters=AudioQuality.STUDIO if video else AudioQuality.STUDIO,
        video_parameters=VideoQuality.HD_720p if video else VideoQuality.SD_360p,
        video_flags=(MediaStream.Flags.AUTO_DETECT if video else MediaStream.Flags.IGNORE),
        ffmpeg_parameters=ffmpeg_params,
    )

async def _clear_(chat_id: int) -> None:
    popped = db.pop(chat_id, None)
    if popped:
        await auto_clean(popped)
    db[chat_id] = []
    await remove_active_video_chat(chat_id)
    await remove_active_chat(chat_id)
    await set_loop(chat_id, 0)

class Call:
    def __init__(self):
        # Client တွေကို အသံထွက်ကြည့်လင် ကောင်းအောင် configure လုပ်ခြင်း
        client_config = {
            "app_version": "1.0.0",
            "device_model": "Desktop",
            "system_version": "Windows 10",
            "lang_code": "en",
            "ipv6": False,
            "proxy": None
        }
        
        self.userbot1 = Client(
            name="maythusharmusic1",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            session_string=config.STRING1,
            **client_config
        ) if config.STRING1 else None
        self.one = PyTgCalls(self.userbot1) if self.userbot1 else None

        self.userbot2 = Client(
            name="maythusharmusic2",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            session_string=config.STRING2,
            **client_config
        ) if config.STRING2 else None
        self.two = PyTgCalls(self.userbot2) if self.userbot2 else None

        self.userbot3 = Client(
            name="maythusharmusic3",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            session_string=config.STRING3,
            **client_config
        ) if config.STRING3 else None
        self.three = PyTgCalls(self.userbot3) if self.userbot3 else None

        self.userbot4 = Client(
            name="maythusharmusic4",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            session_string=config.STRING4,
            **client_config
        ) if config.STRING4 else None
        self.four = PyTgCalls(self.userbot4) if self.userbot4 else None

        self.userbot5 = Client(
            name="maythusharmusic5",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            session_string=config.STRING5,
            **client_config
        ) if config.STRING5 else None
        self.five = PyTgCalls(self.userbot5) if self.userbot5 else None

        self.active_calls: set[int] = set()
        
        # အသံထွက်ကြည့်လင် အတွက် custom parameters
        self.audio_enhancement = {
            "bass_boost": True,
            "noise_reduction": True,
            "volume_normalization": True,
            "equalizer": "pop"  # pop, rock, classical, bass_boost, treble_boost
        }

    def get_enhanced_ffmpeg_params(self, bass_boost: bool = True) -> str:
        """အသံထွက်ကြည့်လင် ပြတ်သားစေရန် FFmpeg parameters"""
        params = []
        
        # Volume ကို အနည်းငယ်မြှင့်ပေးခြင်း
        params.append("volume=1.3")
        
        # Bass boost ထည့်ခြင်း
        if bass_boost and self.audio_enhancement["bass_boost"]:
            params.append("bass=g=8:f=100")
            
        # Equalizer settings
        if self.audio_enhancement["equalizer"] == "pop":
            params.append("equalizer=f=100:width_type=h:width=100:g=5")
            params.append("equalizer=f=1000:width_type=h:width=100:g=3")
            params.append("equalizer=f=5000:width_type=h:width=100:g=2")
        elif self.audio_enhancement["equalizer"] == "bass_boost":
            params.append("equalizer=f=60:width_type=h:width=50:g=10")
            params.append("equalizer=f=170:width_type=h:width=100:g=8")
            
        # Noise reduction
        if self.audio_enhancement["noise_reduction"]:
            params.append("afftdn=nr=20:nf=-25")
            
        # Dynamic audio normalization
        if self.audio_enhancement["volume_normalization"]:
            params.append("dynaudnorm")
            
        # High pass filter to reduce low-frequency noise
        params.append("highpass=f=80")
        
        # Compressor to make audio more consistent
        params.append("compand=attacks=0.3:decays=0.8:points=-80/-80|-40/-15|-20/-12|0/-7")
        
        return f"-af '{','.join(params)}'"

    @capture_internal_err
    async def pause_stream(self, chat_id: int) -> None:
        assistant = await group_assistant(self, chat_id)
        await assistant.pause(chat_id)

    @capture_internal_err
    async def resume_stream(self, chat_id: int) -> None:
        assistant = await group_assistant(self, chat_id)
        await assistant.resume(chat_id)

    @capture_internal_err
    async def mute_stream(self, chat_id: int) -> None:
        assistant = await group_assistant(self, chat_id)
        await assistant.mute(chat_id)

    @capture_internal_err
    async def unmute_stream(self, chat_id: int) -> None:
        assistant = await group_assistant(self, chat_id)
        await assistant.unmute(chat_id)

    @capture_internal_err
    async def stop_stream(self, chat_id: int) -> None:
        assistant = await group_assistant(self, chat_id)
        await _clear_(chat_id)
        if chat_id not in self.active_calls:
            return
        try:
            await assistant.leave_call(chat_id)
        except NoActiveGroupCall:
            pass
        except Exception as e:
            LOGGER(__name__).error(f"Error leaving call: {e}")
        finally:
            self.active_calls.discard(chat_id)

    @capture_internal_err
    async def force_stop_stream(self, chat_id: int) -> None:
        assistant = await group_assistant(self, chat_id)
        try:
            check = db.get(chat_id)
            if check:
                check.pop(0)
        except (IndexError, KeyError):
            pass
        await remove_active_video_chat(chat_id)
        await remove_active_chat(chat_id)
        await _clear_(chat_id)
        if chat_id not in self.active_calls:
            return
        try:
            await assistant.leave_call(chat_id)
        except NoActiveGroupCall:
            pass
        except Exception as e:
            LOGGER(__name__).error(f"Error leaving call in force stop: {e}")
        finally:
            self.active_calls.discard(chat_id)

    @capture_internal_err
    async def skip_stream(self, chat_id: int, link: str, video: Union[bool, str] = None, image: Union[bool, str] = None) -> None:
        assistant = await group_assistant(self, chat_id)
        # အသံထွက်ကြည့်လင် ပြတ်သားစေရန် enhanced parameters
        enhanced_params = self.get_enhanced_ffmpeg_params()
        stream = dynamic_media_stream(
            path=link, 
            video=bool(video),
            ffmpeg_params=enhanced_params
        )
        await assistant.play(chat_id, stream)

    @capture_internal_err
    async def vc_users(self, chat_id: int) -> list:
        assistant = await group_assistant(self, chat_id)
        participants = await assistant.get_participants(chat_id)
        return [p.user_id for p in participants if not p.is_muted]

    @capture_internal_err
    async def change_volume(self, chat_id: int, volume: int) -> None:
        assistant = await group_assistant(self, chat_id)
        # Volume ကို အနည်းငယ်ပိုမြှင့်ပေးခြင်း (clarity အတွက်)
        enhanced_volume = min(200, int(volume * 1.2))  # 20% ပိုမြှင့်ပေးခြင်း
        await assistant.change_volume_call(chat_id, enhanced_volume)

    @capture_internal_err
    async def seek_stream(self, chat_id: int, file_path: str, to_seek: str, duration: str, mode: str) -> None:
        assistant = await group_assistant(self, chat_id)
        # Enhanced audio parameters ထည့်ပေးခြင်း
        enhanced_params = self.get_enhanced_ffmpeg_params()
        ffmpeg_params = f"-ss {to_seek} -to {duration} {enhanced_params}"
        is_video = mode == "video"
        stream = dynamic_media_stream(path=file_path, video=is_video, ffmpeg_params=ffmpeg_params)
        await assistant.play(chat_id, stream)

    @capture_internal_err
    async def speedup_stream(self, chat_id: int, file_path: str, speed: float, playing: list) -> None:
        if not isinstance(playing, list) or not playing or not isinstance(playing[0], dict):
            raise AssistantErr("Invalid stream info for speedup.")

        assistant = await group_assistant(self, chat_id)
        base = os.path.basename(file_path)
        chatdir = os.path.join("playback", str(speed))
        os.makedirs(chatdir, exist_ok=True)
        out = os.path.join(chatdir, base)

        if not os.path.exists(out):
            vs = str(2.0 / float(speed))
            # Enhanced audio processing ထည့်ပေးခြင်း
            audio_filters = self.get_enhanced_ffmpeg_params().replace("-af '", "").replace("'", "")
            cmd = f"ffmpeg -i {file_path} -filter:v setpts={vs}*PTS -filter:a atempo={speed},{audio_filters} {out}"
            
            LOGGER(__name__).info(f"Processing speedup with enhanced audio: {cmd}")
            
            proc = await asyncio.create_subprocess_shell(
                cmd, 
                stdin=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.PIPE
            )
            _, stderr = await proc.communicate()
            
            if proc.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                LOGGER(__name__).error(f"FFmpeg error: {error_msg}")
                raise AssistantErr(f"Failed to process speed change: {error_msg}")

        dur = int(await asyncio.get_event_loop().run_in_executor(None, check_duration, out))
        played, con_seconds = speed_converter(playing[0]["played"], speed)
        duration_min = seconds_to_min(dur)
        is_video = playing[0]["streamtype"] == "video"
        
        # Enhanced parameters for playback
        enhanced_params = self.get_enhanced_ffmpeg_params()
        ffmpeg_params = f"-ss {played} -to {duration_min} {enhanced_params}"
        stream = dynamic_media_stream(path=out, video=is_video, ffmpeg_params=ffmpeg_params)

        if chat_id in db and db[chat_id] and db[chat_id][0].get("file") == file_path:
            await assistant.play(chat_id, stream)
            LOGGER(__name__).info(f"Playing speedup stream with enhanced audio in chat {chat_id}")
        else:
            raise AssistantErr("Stream mismatch during speedup.")

        db[chat_id][0].update({
            "played": con_seconds,
            "dur": duration_min,
            "seconds": dur,
            "speed_path": out,
            "speed": speed,
            "old_dur": db[chat_id][0].get("dur"),
            "old_second": db[chat_id][0].get("seconds"),
        })

    @capture_internal_err
    async def stream_call(self, link: str) -> None:
        assistant = await group_assistant(self, config.LOGGER_ID)
        try:
            # Enhanced audio for test stream
            enhanced_params = self.get_enhanced_ffmpeg_params()
            stream = MediaStream(
                link,
                audio_parameters=AudioQuality.HIGH,
                ffmpeg_parameters=enhanced_params
            )
            await assistant.play(config.LOGGER_ID, stream)
            await asyncio.sleep(8)
        except Exception as e:
            LOGGER(__name__).error(f"Error in stream_call: {e}")
        finally:
            try:
                await assistant.leave_call(config.LOGGER_ID)
            except Exception as e:
                LOGGER(__name__).error(f"Error leaving logger call: {e}")

    @capture_internal_err
    async def join_call(
        self,
        chat_id: int,
        original_chat_id: int,
        link: str,
        video: Union[bool, str] = None,
        image: Union[bool, str] = None,
    ) -> None:
        assistant = await group_assistant(self, chat_id)
        lang = await get_lang(chat_id)
        _ = get_string(lang)
        
        # Enhanced audio parameters
        enhanced_params = self.get_enhanced_ffmpeg_params()
        stream = dynamic_media_stream(
            path=link, 
            video=bool(video),
            ffmpeg_params=enhanced_params
        )

        try:
            await assistant.play(chat_id, stream)
            LOGGER(__name__).info(f"Joined call in chat {chat_id} with enhanced audio")
        except (NoActiveGroupCall, ChatAdminRequired):
            raise AssistantErr(_["call_8"])
        except TelegramServerError:
            raise AssistantErr(_["call_10"])
        except Exception as e:
            LOGGER(__name__).error(f"Join call error: {traceback.format_exc()}")
            raise AssistantErr(
                f"ᴜɴᴀʙʟᴇ ᴛᴏ ᴊᴏɪɴ ᴛʜᴇ ɢʀᴏᴜᴘ ᴄᴀʟʟ.\nRᴇᴀsᴏɴ: {e}"
            )
        
        self.active_calls.add(chat_id)
        await add_active_chat(chat_id)
        await music_on(chat_id)
        if video:
            await add_active_video_chat(chat_id)

        if await is_autoend():
            counter[chat_id] = {}
            users = len(await assistant.get_participants(chat_id))
            if users == 1:
                autoend[chat_id] = datetime.now() + timedelta(minutes=1)

    @capture_internal_err
    async def play(self, client, chat_id: int) -> None:
        # ပထမဆုံး check လုပ်ရန် db မှာ chat_id ရှိမရှိ စစ်ဆေးပါ
        if chat_id not in db or not db.get(chat_id):
            LOGGER(__name__).warning(f"No queue found for chat {chat_id}, cleaning up...")
            await _clear_(chat_id)
            if chat_id in self.active_calls:
                try:
                    await client.leave_call(chat_id)
                except NoActiveGroupCall:
                    pass
                except Exception as e:
                    LOGGER(__name__).error(f"Error leaving call: {e}")
                finally:
                    self.active_calls.discard(chat_id)
            return
        
        check = db.get(chat_id)
        popped = None
        loop = await get_loop(chat_id)
        
        try:
            # check list က empty မဖြစ်ကြောင်း သေချာအောင်လုပ်ပါ
            if not check or len(check) == 0:
                LOGGER(__name__).warning(f"Empty queue for chat {chat_id}, cleaning up...")
                await _clear_(chat_id)
                if chat_id in self.active_calls:
                    try:
                        await client.leave_call(chat_id)
                    except NoActiveGroupCall:
                        pass
                    except Exception as e:
                        LOGGER(__name__).error(f"Error leaving call: {e}")
                    finally:
                        self.active_calls.discard(chat_id)
                return
            
            if loop == 0:
                popped = check.pop(0)
            else:
                loop = loop - 1
                await set_loop(chat_id, loop)
            
            if popped:
                await auto_clean(popped)
            
            if not check:
                await _clear_(chat_id)
                if chat_id in self.active_calls:
                    try:
                        await client.leave_call(chat_id)
                    except NoActiveGroupCall:
                        pass
                    except Exception as e:
                        LOGGER(__name__).error(f"Error leaving call in play: {e}")
                    finally:
                        self.active_calls.discard(chat_id)
                return
        except IndexError as e:
            LOGGER(__name__).warning(f"IndexError in play for chat {chat_id}: {e}")
            await _clear_(chat_id)
            if chat_id in self.active_calls:
                try:
                    await client.leave_call(chat_id)
                except Exception as e2:
                    LOGGER(__name__).error(f"Error leaving call: {e2}")
                finally:
                    self.active_calls.discard(chat_id)
            return
        except Exception as e:
            LOGGER(__name__).error(f"Error in play cleanup: {traceback.format_exc()}")
            try:
                await _clear_(chat_id)
                await client.leave_call(chat_id)
            except Exception as e2:
                LOGGER(__name__).error(f"Error in final cleanup: {e2}")
            finally:
                if chat_id in self.active_calls:
                    self.active_calls.discard(chat_id)
            return
            
        try:
            # check list ထဲမှာ item တွေရှိမရှိ ထပ်မံစစ်ဆေးပါ
            if not check or len(check) == 0:
                LOGGER(__name__).warning(f"No items in queue after pop for chat {chat_id}")
                await _clear_(chat_id)
                if chat_id in self.active_calls:
                    try:
                        await client.leave_call(chat_id)
                    except Exception:
                        pass
                    finally:
                        self.active_calls.discard(chat_id)
                return
                
            queued = check[0]["file"]
            language = await get_lang(chat_id)
            _ = get_string(language)
            title = (check[0]["title"]).title()
            user = check[0]["by"]
            original_chat_id = check[0]["chat_id"]
            streamtype = check[0]["streamtype"]
            videoid = check[0]["vidid"]
            db[chat_id][0]["played"] = 0

            exis = (check[0]).get("old_dur")
            if exis:
                db[chat_id][0]["dur"] = exis
                db[chat_id][0]["seconds"] = check[0]["old_second"]
                db[chat_id][0]["speed_path"] = None
                db[chat_id][0]["speed"] = 1.0

            video = True if str(streamtype) == "video" else False
            
            # Enhanced audio parameters for all stream types
            enhanced_params = self.get_enhanced_ffmpeg_params()

            if "live_" in queued:
                n, link = await YouTube.video(videoid, True)
                if n == 0:
                    return await app.send_message(original_chat_id, text=_["call_6"])

                stream = dynamic_media_stream(
                    path=link, 
                    video=video,
                    ffmpeg_params=enhanced_params
                )
                try:
                    await client.play(chat_id, stream)
                    LOGGER(__name__).info(f"Playing live stream with enhanced audio in chat {chat_id}")
                except Exception as e:
                    LOGGER(__name__).error(f"Live stream play error: {e}")
                    return await app.send_message(original_chat_id, text=_["call_6"])

                img = await get_thumb(videoid)
                button = stream_markup(_, chat_id)
                run = await app.send_photo(
                    chat_id=original_chat_id,
                    photo=img,
                    caption=_["stream_1"].format(
                        f"https://t.me/{app.username}?start=info_{videoid}",
                        title[:23],
                        check[0]["dur"],
                        user,
                    ),
                    reply_markup=InlineKeyboardMarkup(button),
                )
                db[chat_id][0]["mystic"] = run
                db[chat_id][0]["markup"] = "tg"

            elif "vid_" in queued:
                mystic = await app.send_message(original_chat_id, _["call_7"])
                try:
                    file_path, direct = await YouTube.download(
                        videoid,
                        mystic,
                        videoid=True,
                        video=True if str(streamtype) == "video" else False,
                    )
                except Exception as e:
                    LOGGER(__name__).error(f"Video download error: {e}")
                    return await mystic.edit_text(
                        _["call_6"], disable_web_page_preview=True
                    )

                stream = dynamic_media_stream(
                    path=file_path, 
                    video=video,
                    ffmpeg_params=enhanced_params
                )
                try:
                    await client.play(chat_id, stream)
                    LOGGER(__name__).info(f"Playing video with enhanced audio in chat {chat_id}")
                except Exception as e:
                    LOGGER(__name__).error(f"Video stream play error: {e}")
                    return await app.send_message(original_chat_id, text=_["call_6"])

                img = await get_thumb(videoid)
                button = stream_markup(_, chat_id)
                await mystic.delete()
                run = await app.send_photo(
                    chat_id=original_chat_id,
                    photo=img,
                    caption=_["stream_1"].format(
                        f"https://t.me/{app.username}?start=info_{videoid}",
                        title[:23],
                        check[0]["dur"],
                        user,
                    ),
                    reply_markup=InlineKeyboardMarkup(button),
                )
                db[chat_id][0]["mystic"] = run
                db[chat_id][0]["markup"] = "stream"

            elif "index_" in queued:
                stream = dynamic_media_stream(
                    path=videoid, 
                    video=video,
                    ffmpeg_params=enhanced_params
                )
                try:
                    await client.play(chat_id, stream)
                    LOGGER(__name__).info(f"Playing index stream with enhanced audio in chat {chat_id}")
                except Exception as e:
                    LOGGER(__name__).error(f"Index stream play error: {e}")
                    return await app.send_message(original_chat_id, text=_["call_6"])

                button = stream_markup(_, chat_id)
                run = await app.send_photo(
                    chat_id=original_chat_id,
                    photo=config.STREAM_IMG_URL,
                    caption=_["stream_2"].format(user),
                    reply_markup=InlineKeyboardMarkup(button),
                )
                db[chat_id][0]["mystic"] = run
                db[chat_id][0]["markup"] = "tg"

            else:
                stream = dynamic_media_stream(
                    path=queued, 
                    video=video,
                    ffmpeg_params=enhanced_params
                )
                try:
                    await client.play(chat_id, stream)
                    LOGGER(__name__).info(f"Playing audio with enhanced quality in chat {chat_id}")
                except Exception as e:
                    LOGGER(__name__).error(f"Audio stream play error: {e}")
                    return await app.send_message(original_chat_id, text=_["call_6"])

                if videoid == "telegram":
                    button = stream_markup(_, chat_id)
                    run = await app.send_photo(
                        chat_id=original_chat_id,
                        photo=(
                            config.TELEGRAM_AUDIO_URL
                            if str(streamtype) == "audio"
                            else config.TELEGRAM_VIDEO_URL
                        ),
                        caption=_["stream_1"].format(
                            config.SUPPORT_CHAT, title[:23], check[0]["dur"], user
                        ),
                        reply_markup=InlineKeyboardMarkup(button),
                    )
                    db[chat_id][0]["mystic"] = run
                    db[chat_id][0]["markup"] = "tg"

                elif videoid == "soundcloud":
                    button = stream_markup(_, chat_id)
                    run = await app.send_photo(
                        chat_id=original_chat_id,
                        photo=config.SOUNCLOUD_IMG_URL,
                        caption=_["stream_1"].format(
                            config.SUPPORT_CHAT, title[:23], check[0]["dur"], user
                        ),
                        reply_markup=InlineKeyboardMarkup(button),
                    )
                    db[chat_id][0]["mystic"] = run
                    db[chat_id][0]["markup"] = "tg"

                else:
                    img = await get_thumb(videoid)
                    button = stream_markup(_, chat_id)
                    try:
                        run = await app.send_photo(
                            chat_id=original_chat_id,
                            photo=img,
                            caption=_["stream_1"].format(
                                f"https://t.me/{app.username}?start=info_{videoid}",
                                title[:23],
                                check[0]["dur"],
                                user,
                            ),
                            reply_markup=InlineKeyboardMarkup(button),
                        )
                    except FloodWait as e:
                        LOGGER(__name__).warning(f"FloodWait: Sleeping for {e.value}")
                        await asyncio.sleep(e.value)
                        run = await app.send_photo(
                            chat_id=original_chat_id,
                            photo=img,
                            caption=_["stream_1"].format(
                                f"https://t.me/{app.username}?start=info_{videoid}",
                                title[:23],
                                check[0]["dur"],
                                user,
                            ),
                            reply_markup=InlineKeyboardMarkup(button),
                        )
                    db[chat_id][0]["mystic"] = run
                    db[chat_id][0]["markup"] = "stream"
                    
        except Exception as e:
            LOGGER(__name__).error(f"Error in play function: {traceback.format_exc()}")
            try:
                await _clear_(chat_id)
                await client.leave_call(chat_id)
            except Exception:
                pass
            finally:
                if chat_id in self.active_calls:
                    self.active_calls.discard(chat_id)

    async def start(self) -> None:
        LOGGER(__name__).info("Starting PyTgCalls Clients with enhanced audio...")
        try:
            if config.STRING1:
                await self.one.start()
                LOGGER(__name__).info("Client 1 started")
            if config.STRING2:
                await self.two.start()
                LOGGER(__name__).info("Client 2 started")
            if config.STRING3:
                await self.three.start()
                LOGGER(__name__).info("Client 3 started")
            if config.STRING4:
                await self.four.start()
                LOGGER(__name__).info("Client 4 started")
            if config.STRING5:
                await self.five.start()
                LOGGER(__name__).info("Client 5 started")
        except Exception as e:
            LOGGER(__name__).error(f"Error starting clients: {e}")

    @capture_internal_err
    async def ping(self) -> str:
        pings = []
        if config.STRING1:
            pings.append(self.one.ping)
        if config.STRING2:
            pings.append(self.two.ping)
        if config.STRING3:
            pings.append(self.three.ping)
        if config.STRING4:
            pings.append(self.four.ping)
        if config.STRING5:
            pings.append(self.five.ping)
        return str(round(sum(pings) / len(pings), 3)) if pings else "0.0"

    @capture_internal_err
    async def decorators(self) -> None:
        assistants = list(filter(None, [self.one, self.two, self.three, self.four, self.five]))

        CRITICAL_FLAGS = (
            ChatUpdate.Status.KICKED |
            ChatUpdate.Status.LEFT_GROUP |
            ChatUpdate.Status.CLOSED_VOICE_CHAT |
            ChatUpdate.Status.DISCARDED_CALL |
            ChatUpdate.Status.BUSY_CALL
        )

        async def unified_update_handler(client, update: Update) -> None:
            try:
                if isinstance(update, ChatUpdate):
                    if update.status & ChatUpdate.Status.LEFT_CALL or update.status & CRITICAL_FLAGS:
                        await self.stop_stream(update.chat_id)
                        return

                elif isinstance(update, StreamEnded) and update.stream_type == StreamEnded.Type.AUDIO:
                    assistant = await group_assistant(self, update.chat_id)
                    await self.play(assistant, update.chat_id)

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                full_trace = "".join(traceback.format_exception(exc_type, exc_obj, exc_tb))
                caption = (
                    f"🚨 <b>Stream Update Error</b>\n"
                    f"📍 <b>Update Type:</b> <code>{type(update).__name__}</code>\n"
                    f"📍 <b>Error Type:</b> <code>{exc_type.__name__}</code>"
                )
                filename = f"update_error_{getattr(update, 'chat_id', 'unknown')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                await send_large_error(full_trace, caption, filename)

        for assistant in assistants:
            assistant.on_update()(unified_update_handler)
            
    @capture_internal_err
    async def set_audio_quality(self, bass_boost: bool = None, equalizer: str = None) -> None:
        """အသံအရည်အသွေးကို ပြင်ဆင်ရန် function"""
        if bass_boost is not None:
            self.audio_enhancement["bass_boost"] = bass_boost
        if equalizer is not None and equalizer in ["pop", "rock", "classical", "bass_boost", "treble_boost"]:
            self.audio_enhancement["equalizer"] = equalizer
        
        LOGGER(__name__).info(f"Audio settings updated: {self.audio_enhancement}")


Hotty = Call()
