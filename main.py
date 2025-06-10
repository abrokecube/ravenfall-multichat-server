import asyncio
import aiohttp.client_exceptions
import twitchio
from twitchio import eventsub
from twitchio.ext import routines
import json
from websockets.asyncio.server import serve
from websockets import ServerConnection
from websockets.exceptions import ConnectionClosedError
from typing import List, Dict
import logging
from datetime import timedelta, datetime, timezone
import ravenpy
from ravenpy import Skills, Islands
import re
import os
from dotenv import load_dotenv
from utils import utils, langstuff
from dataclasses import dataclass
import time
import math
import aiohttp
import random

load_dotenv()

# http://localhost:4343/oauth?scopes=user:read:chat%20user:write:chat%20user:bot
# http://localhost:4343/oauth?scopes=user:read:chat%20user:write:chat%20user:bot%20channel:manage:moderators

LOGGER: logging.Logger = logging.getLogger("Bot")
BOT_ID = 756734432
CLIENT_ID = "***REMOVED***"
CLIENT_SECRET = "***REMOVED***"

# From https://lospec.com/palette-list/bubblegum-16
COLORS = [
    # "#7f0622", 
    "#ff8426", 
    "#10d275", 
    "#94216a", 
    "#ffd100", 
    # "#430067", 
    "#007899", 
    # "#234975", 
    "#ff80a4", 
    "#68aed4", 
    "#bfff3c", 
    "#ff2674", 
    "#d62411", 
    # "#002859", 
]

@dataclass
class ChannelInfo:
    id: str
    name: str = ""
    category: str = ""
    display_name: str = ""
    live: bool = False

ws: ServerConnection
async def ws_handler(ws_: ServerConnection):
    global ws
    global twitch_client
    ws = ws_
    async for data in ws_:
        print("Recieved: ", data)
        message = json.loads(data)
        match message['type']:
            case "send_message":
                await twitch_client.send_chat_message(
                    message['user'], message['channel'], message['text']
                )
            case "join":
                await twitch_client.join_channel(username=message['channel'])
            case "add_moderator":
                target_user = await twitch_client.get_id(message['user'])
                for ch_id, channel in twitch_client.user_info.items():
                    if channel['can_add_moderators']:
                        await asyncio.sleep(0.15)
                        a = twitch_client.create_partialuser(ch_id)
                        try:
                            await a.add_moderator(target_user)
                        except twitchio.exceptions.HTTPException as e:
                            await twitch_client.send_system_message(f"Failed to add {message['user']} as mod in {channel['name']}: {e.extra['message']}")
                            continue
                        await twitch_client.send_system_message(f"Added {message['user']} as mod in {channel['name']}")
        ...

async def send_ws(message, text: bool=False):
    try:
        await ws.send(
            message, text
        )
        # print(message)
    except ConnectionClosedError:
        logging.warning("WS connection to rfchatter is closed.")
        pass
    except NameError:
        logging.warning("Waiting for WS connection to rfchatter.")
        pass

async def ws_serve_task():
    print("Serving WS server...")
    async with serve(ws_handler, "", 9832) as server:
        await server.serve_forever()

async def get_characters(rfapi: ravenpy.RavenNest, user_id: str=None):
    tasks = [
        rfapi.get_character(user_id,'1'),
        rfapi.get_character(user_id,'2'),
        rfapi.get_character(user_id,'3')
    ]
    user_chars = await asyncio.gather(*tasks, return_exceptions=False)
    user_chars = [x for x in user_chars if isinstance(x, ravenpy.Character)]
    return user_chars


class ChatClient(twitchio.Client):
    def __init__(self, rf_api: ravenpy.RavenNest=None):
        super().__init__(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, bot_id=BOT_ID)
        ws_task = asyncio.create_task(ws_serve_task())
        self.account_channels: Dict[str, List[str]] = {}
        self.connected_channels: Dict[str, ChannelInfo] = {}
        self.user_info: Dict[str, Dict[str]] = {}
        self.ravenfall_users: List[str] = []
        self.username_to_id: Dict[str, str] = {}
        self.id_to_username: Dict[str, str] = {}
        self.mention_re: re.Pattern = None
        self.rf_api = rf_api
        self.ran_once = False
        self.player_char_data: Dict[str, ravenpy.Character] = {}
        self.current_mult = 1
        self.random_leaves: Dict[str, list[ravenpy.Character]] = {}
        self.desync_per_channel_id: Dict[str, List[float]] = {}
    
    async def get_username(self, user_id: str):
        user_id = str(user_id)
        if not user_id in self.username_to_id:
            user = await self.fetch_users(ids=[user_id])
            self.username_to_id[user[0].name] = user_id
            self.id_to_username[user_id] = user[0].name
        return self.id_to_username[user_id]

    async def get_id(self, username: str):
        username = username.lower()
        if not username in self.id_to_username:
            user = await self.fetch_users(logins=[username])
            self.id_to_username[user[0].id] = username
            self.username_to_id[username] = user[0].id
        return self.username_to_id[username]

    async def setup_hook(self) -> None:
        if self.ran_once:
            return
        self.ran_once = True
        with open("channels.json", 'r') as f:
            self.account_channels = json.load(f)
        with open("users.json", 'r') as f:
            self.user_info = json.load(f)
            
        for channel_ids in self.account_channels.values():
            self.connected_channels.update([(x, ChannelInfo(x)) for x in channel_ids])
        
        if None in self.connected_channels:
            self.connected_channels.pop(None)
        channels_data = await self.fetch_channels(list(self.connected_channels.keys()))
        for channel_data in channels_data:
            channel = self.connected_channels[channel_data.user.id]
            channel.category = channel_data.game_name
            channel.name = channel_data.user.name
            channel.display_name = channel_data.user.display_name
            self.username_to_id[channel.name.lower()] = channel.id
            self.id_to_username[str(channel.id)] = channel.name
        
        for channel_id in self.connected_channels:
            payload = eventsub.ChatMessageSubscription(
                broadcaster_user_id=str(channel_id), user_id=str(self.bot_id)
            )
            await self.subscribe_websocket(payload=payload, token_for=self.bot_id)
            payload = eventsub.ChannelUpdateSubscription(
                broadcaster_user_id=str(channel_id)
            )
            await self.subscribe_websocket(payload=payload, token_for=self.bot_id)
        self.refresh_users()
        self.fetch_rf_api.start()
        self.send_channels.start()
        self.resync_routine.start()
        LOGGER.info("Finished setup hook!")
    
    def save_channels(self):
        with open("channels.json", 'w') as f:
            json.dump(self.account_channels, f)
    
    async def join_channel(self, *, id: str=None, username: str=None):
        if id is None:
            user = (await self.fetch_users(logins=[username]))[0]
            channel = await self.fetch_channel(broadcaster_id=user.id)
            id = user.id
        if username is None:
            channel = await self.fetch_channel(broadcaster_id=id)
            username = channel.user.name
        self.username_to_id[username.lower()] = id
        self.id_to_username[str(id)] = username

        payload = eventsub.ChatMessageSubscription(
            broadcaster_user_id=str(id), user_id=str(self.bot_id)
        )
        await self.subscribe_websocket(payload=payload, token_for=self.bot_id)
        payload = eventsub.ChannelUpdateSubscription(
            broadcaster_user_id=str(id)
        )
        await self.subscribe_websocket(payload=payload, token_for=self.bot_id)
        self.connected_channels[id] = ChannelInfo(id, username, channel.game_name, channel.user.display_name)
        await self.send_system_message(f"Joined {channel.user.display_name}")
    
    async def add_token(self, token, refresh):
        resp: twitchio.authentication.ValidateTokenPayload = await super().add_token(token, refresh)
        user_info = await self.fetch_users(ids=[resp.user_id])
        self.user_info[resp.user_id] = {
            "name": user_info[0].name,
            "can_add_moderators": False,
            "can_execute_commands": False
        }
        with open("users.json", 'w') as f:
            json.dump(self.user_info, f)
        self.refresh_users()
        return resp
    
    def refresh_users(self):
        self.ravenfall_users = []
        for id, user in self.user_info.items():
            self.username_to_id[user['name']] = id
            self.id_to_username[id] = user["name"]
            self.ravenfall_users.append(id)
            if not id in self.account_channels:
                self.account_channels[id] = []
            if len(self.account_channels[id]) < 3:
                while len(self.account_channels[id]) < 3:
                    self.account_channels[id].append(None)
        mention_re_text = '|'.join([f'(^|\\s)@?{x}(?=\\W|$)' for x in self.username_to_id.keys()])
        self.mention_re = re.compile(mention_re_text)
        self.save_channels()
        print(mention_re_text)
    
    async def handle_sail(self, channel_id: str, channel_name: str):
        for char in self.player_char_data.values():
            if char.training in (None, Skills.Sailing):
                if char.in_dungeon or char.in_raid:
                    continue
                if char.in_onsen:
                    await self.send_chat_message(char.user_name, channel_name, '!rest leave')
                    await asyncio.sleep(2)
                channel = self.account_channels[char.twitch_id][char.index-1]
                if channel == channel_id:
                    await self.send_chat_message(char.user_name, channel_name, '!sail')
                    await asyncio.sleep(0.5)

    async def handle_random_leave(self, channel_id: str, channel_name: str):
        if channel_id not in self.random_leaves:
            self.random_leaves[channel_id] = []
        char_data_list = list(self.player_char_data.values())
        while True:
            char = random.choice(char_data_list)
            channel = self.account_channels[char.twitch_id][char.index-1]
            if channel == channel_id:
                await self.send_chat_message(char.user_name, channel_name, '!leave')
                self.random_leaves[channel_id].append(char)
                return

    async def handle_random_relog(self, channel_id: str, channel_name: str):
        if channel_id not in self.random_leaves:
            self.random_leaves[channel_id] = []
        char_data_list = list(self.player_char_data.values())
        while True:
            char = random.choice(char_data_list)
            channel = self.account_channels[char.twitch_id][char.index-1]
            if channel == channel_id:
                await self.send_chat_message(char.user_name, channel_name, '!leave')
                await asyncio.sleep(1)
                await self.send_chat_message(char.user_name, channel_name, f'!join {char.index}')
                return

    async def handle_undo_random_leave(self, channel_id: str, channel_name: str):
        if channel_id not in self.random_leaves or not self.random_leaves[channel_id]:
            await self.send_chat_message(channel_name, channel_name, "No random leaves to undo")
            return
        for char in self.random_leaves[channel_id]:
            await self.send_chat_message(char.user_name, channel_name, f'!join {char.index}')
            await asyncio.sleep(0.5)
        self.random_leaves[channel_id] = []

    async def handle_ping(self, channel_id: str, channel_name: str):
        char_data_list = list(self.player_char_data.values())
        while True:
            char = random.choice(char_data_list)
            channel = self.account_channels[char.twitch_id][char.index-1]
            if channel == channel_id:
                await self.send_chat_message(char.user_name, channel_name, f'[rf-multichat] Pong! Watching {len(char_data_list)} characters.')
                return

    async def handle_raid(self, channel_id: str, channel_name: str):
        for char in self.player_char_data.values():
            if not char.training in ravenpy.combat_skills:
                continue
            if char.auto_join_raid_count == 0:
                channel = self.account_channels[char.twitch_id][char.index-1]
                if channel == channel_id:
                    await self.send_chat_message(char.user_name, channel_name, '!auto raid on')
                    await asyncio.sleep(0.4)
                    
    async def handle_dungeon_scroll(self, channel_id: str, channel_name: str):
        user_in_channel = ""
        for char in self.player_char_data.values():
            channel = self.account_channels[char.twitch_id][char.index-1]
            if channel != channel_id:
                continue
            if not user_in_channel:
                user_in_channel = char.user_name
            dscroll = char.get_item(ravenpy.Items.DungeonScroll)
            if dscroll:
                await self.send_chat_message(char.user_name, channel_name, '!dungeon start')
                return
        else:
            await self.send_chat_message(user_in_channel, channel_name, 'Out of dungeon scrolls :(')
            
    async def handle_raid_scroll(self, channel_id: str, channel_name: str):
        user_in_channel = ""
        for char in self.player_char_data.values():
            channel = self.account_channels[char.twitch_id][char.index-1]
            if channel != channel_id:
                continue
            if not user_in_channel:
                user_in_channel = char.user_name
            dscroll = char.get_item(ravenpy.Items.RaidScroll)
            if dscroll:
                await self.send_chat_message(char.user_name, channel_name, '!raid start')
                break
        else:
            await self.send_chat_message(user_in_channel, channel_name, 'Out of raid scrolls :(')

    async def handle_ferry_scroll(self, channel_id: str, channel_name: str):
        user_in_channel = ""
        for char in self.player_char_data.values():
            channel = self.account_channels[char.twitch_id][char.index-1]
            if channel != channel_id:
                continue
            if not user_in_channel:
                user_in_channel = char.user_name
            dscroll = char.get_item("Ferry Scroll")
            if dscroll:
                await self.send_chat_message(char.user_name, channel_name, '!ferry boost')
                break
        else:
            await self.send_chat_message(user_in_channel, channel_name, 'Out of ferry scrolls :(')
            
    async def handle_exp_scroll(self, channel_id: str, channel_name: str, scroll_count: int, caller_username: str):
        current_count = 0
        user_to_speak = ""
        for char in self.player_char_data.values():
            if (not user_to_speak) and (char.user_name != caller_username):
                user_to_speak = char.user_name
                break
        if self.current_mult >= 100:
            await self.send_chat_message(user_to_speak, channel_name, "Multiplier is already maxed")
            return
        if scroll_count <= 0:
            await self.send_chat_message(user_to_speak, channel_name, "I am keeping my scrolls I guess")
            return
        for char in self.player_char_data.values():
            if char.user_name.lower() == caller_username.lower():
                continue
            if current_count >= scroll_count:
                break
            channel = self.account_channels[char.twitch_id][char.index-1]
            char_channel_name = await self.get_username(channel)
            expscroll = char.get_item(ravenpy.Items.ExpMultiplierScroll)
            if expscroll:
                count = min(scroll_count-current_count, expscroll.amount)
                await self.send_chat_message(char.user_name, char_channel_name, f'!exp {count}')
                current_count += count
                await asyncio.sleep(.75)
        if current_count == 0:
            await self.send_chat_message(user_to_speak, channel_name, "No scrolls :(")
        elif current_count < scroll_count:
            await self.send_chat_message(user_to_speak, channel_name, "Ran out of scrolls")
        else:
            await self.send_chat_message(user_to_speak, channel_name, "Okay")
            
    async def handle_count_scrolls(self, channel_id: str, channel_name: str, caller_username: str, total_scrolls = False):
        scroll_counts = {}
        user_to_speak = ""
        for char in self.player_char_data.values():
            channel = self.account_channels[char.twitch_id][char.index-1]
            dscroll = char.get_item(ravenpy.Items.DungeonScroll)
            rscroll = char.get_item(ravenpy.Items.RaidScroll)
            expscroll = char.get_item(ravenpy.Items.ExpMultiplierScroll)
            fscroll = char.get_item("Ferry Scroll")
            if (not user_to_speak) and (char.user_name != caller_username):
                user_to_speak = char.user_name
            for item in (dscroll, rscroll, expscroll, fscroll):
                if not item: 
                    continue
                if not item.item.name in scroll_counts:
                    scroll_counts[item.item.name] = [0, 0]
                scroll_counts[item.item.name][0] += item.amount
                if channel == channel_id:
                    scroll_counts[item.item.name][1] += item.amount
        scroll_counts_list = [(x, y) for x, y in scroll_counts.items()]
        scroll_counts_list.sort(key=lambda x: x[0])
        # scroll_counts_text = ', '.join(f"{name}: {channel_c}x ({total_c}x)" for name, (total_c, channel_c) in scroll_counts_list)
        if total_scrolls:
            scroll_counts_text = ', '.join(f"{total_c} {utils.pl(total_c, name, False)}" for name, (total_c, channel_c) in scroll_counts_list)
            await self.send_chat_message(
                user_to_speak, channel_name, f"Total available scrolls across channels - {scroll_counts_text}"
            )
        else:
            scroll_counts_text_strs = []
            for name, (total_c, channel_c) in scroll_counts_list:
                if name == "Exp Multiplier Scroll":
                    scroll_counts_text_strs.append(f"{total_c} {utils.pl(channel_c, name, False)}")
                else:
                    scroll_counts_text_strs.append(f"{channel_c} {utils.pl(channel_c, name, False)}")
            scroll_counts_text = ', '.join(scroll_counts_text_strs)
            await self.send_chat_message(
                user_to_speak, channel_name, f"Available scrolls - {scroll_counts_text}"
            )
            

    async def handle_exec_as_joined(self, channel_id: str, channel_name: str, caller_username: str, text: str):
        user_in_channel = ""
        for char in self.player_char_data.values():
            channel = self.account_channels[char.twitch_id][char.index-1]
            if channel != channel_id:
                continue
            if (not user_in_channel) and (char.user_name != caller_username):
                user_in_channel = char.user_name
                break
        await self.send_chat_message(
            user_in_channel, channel_name, text
        )


    async def process_commands(self, payload: twitchio.ChatMessage):
        if payload.text[0] in ("!", ">"):
            await self.parse_chat_command(payload.text, payload.chatter.id, payload.broadcaster.id)
            return
        prefix = "?"
        if len(payload.text) < len(prefix) + 1 or payload.text[0] != "?":
            return
        spl = payload.text[len(prefix):].split()
        command = spl[0].lower()
        args = spl[1:]
        if self.user_info.get(payload.chatter.id, {}).get("can_execute_commands", False):
            match command:
                case "sailall":
                    await self.handle_sail(payload.broadcaster.id, payload.broadcaster.name)
                case "raidall":
                    await self.handle_raid(payload.broadcaster.id, payload.broadcaster.name)
                case "sail":
                    await self.handle_exec_as_joined(
                        payload.broadcaster.id, payload.broadcaster.name, 
                        payload.chatter.name, f"!sail {' '.join(args)}"
                    )
                case "say":
                    await self.handle_exec_as_joined(
                        payload.broadcaster.id, payload.broadcaster.name, 
                        payload.chatter.name, ' '.join(args)
                    )
                case "randleave":
                    await self.handle_random_leave(payload.broadcaster.id, payload.broadcaster.name)
                case "undorandleave":
                    await self.handle_undo_random_leave(payload.broadcaster.id, payload.broadcaster.name)
                case "randleaveundo":
                    await self.handle_undo_random_leave(payload.broadcaster.id, payload.broadcaster.name)
                case "resynctest":
                    await self.resync_routine()
        if self.user_info.get(payload.broadcaster.id, {}).get("can_add_moderators", False):
            match command:
                case "scrolls":
                    get_total = False
                    if args and args[0].lower() == "all":
                        get_total = True
                    await self.handle_count_scrolls(payload.broadcaster.id, payload.broadcaster.name, payload.chatter.name, get_total)
                case "ds":
                    await self.handle_dungeon_scroll(payload.broadcaster.id, payload.broadcaster.name)
                case "rs":
                    await self.handle_raid_scroll(payload.broadcaster.id, payload.broadcaster.name)
                case "fs":
                    await self.handle_ferry_scroll(payload.broadcaster.id, payload.broadcaster.name)
                case "exps":
                    scroll_count = 100 - self.current_mult
                    if len(args) > 0 and args[0].isdigit():
                        scroll_count = int(args[0])
                    await self.handle_exp_scroll(payload.broadcaster.id, payload.broadcaster.name, scroll_count, payload.chatter.name)
                case "resync":
                    await self.handle_random_relog(payload.broadcaster.id, payload.broadcaster.name)
                case "randrelog":
                    await self.handle_random_relog(payload.broadcaster.id, payload.broadcaster.name)
                case "relog":
                    await self.handle_random_relog(payload.broadcaster.id, payload.broadcaster.name)
        match command:
            case "ping":
                await self.handle_ping(payload.broadcaster.id, payload.broadcaster.name)

    _action_re = re.compile("^\u0001?ACTION ")
    async def event_message(self, payload: twitchio.ChatMessage):
        print(f"#{payload.broadcaster.name}: {payload.chatter.name}: {payload.text}")
        # aga = self.mention_re.search(payload.text)
        aga = True
        if aga:
            color = "7f7f7f"
            if payload.chatter.color:
                color = payload.chatter.color.hex_clean
            text = payload.text
            me = False
            if self._action_re.match(text):
                me = True
                text = self._action_re.sub("", text)
            await send_ws(json.dumps({
                "type": "message",
                "channel": payload.broadcaster.display_name,
                "user": payload.chatter.display_name,
                "text": text,
                "user_color": color,
                "me": me,
                "is_broadcaster": payload.chatter.broadcaster,
                "is_moderator": payload.chatter.moderator,
                "is_vip": payload.chatter.vip,
            }))
        await self.process_commands(payload)
            
    async def event_channel_update(self, payload: twitchio.ChannelUpdate):
        channel = self.connected_channels[payload.broadcaster.id]
        channel.name = payload.broadcaster.name
        channel.category = payload.category_name
        self.send_channels.restart()

    async def send_system_message(self, text):
        await send_ws(json.dumps({
            "type": "message",
            "channel": "__sys",
            "user": "__sys",
            "text": text,
            "user_color": "000000",
            "me": False,
            "is_broadcaster": False,
            "is_moderator": False,
            "is_vip": False,
        }))

    async def parse_chat_command(self, text: str, user_id: str, channel_id: str):
        username = await self.get_username(user_id)
        args = text.lstrip("!>").split()
        command = args[0]
        args = args[1:]
        match command:
            case "join":
                if not user_id in self.account_channels:
                    return
                index = 1
                if len(args) > 0:
                    if args[0].isdigit():
                        index = int(args[0])
                    else:
                        for i in range(1, 4):
                            key = f"{username}_{i}"
                            if key in self.player_char_data:
                                char_name = self.player_char_data[key].name
                                if char_name and char_name.split()[0].lower() == args[0]:
                                    index = i
                                    break
                        else:
                            return
                if index == 0:
                    index = 1
                if index < 0:
                    return
                if index <= len(self.account_channels[user_id]):
                    self.account_channels[user_id][index-1] = channel_id
                    self.save_channels()
    
    async def send_chat_message(self, user: str, channel: str, text: str):
        if channel.lower() == "abrokecube":
            text = text[:2].replace("!", ">") + text[2:]
        if user.lower() == "potatbotat":
            text = "-pb " + text
            user = "abrokecube"
        elif user.lower() == "fossabot":
            text = "!fossa " + text
            user = "abrokecube"
        elif user.lower() == "streamelements":
            text = "!se " + text
            user = "abrokecube"
        if not channel.lower() in self.username_to_id:
            await self.send_system_message(f"Not joined in {channel}")
            return
        channel_id = self.username_to_id[channel.lower()]
        if not user.lower() in self.username_to_id:
            await self.send_system_message(f"{user} is not logged in!")
            return
        user_id = self.username_to_id[user.lower()]
        try:
            await self.create_partialuser(channel_id).send_message(text, user_id, token_for=user_id)
        except twitchio.MessageRejectedError as e:
            await self.send_system_message(e.message)
        else:
            if text[0] in ("!", ">"):
                await self.parse_chat_command(text, user_id, channel_id)

    @routines.routine(delta=timedelta(seconds=5))
    async def send_channels(self):
        try:
            await ws.ping()
        except:
            return
        data = []
        for idx, channel_key in enumerate(self.connected_channels.keys()):
            channel = self.connected_channels[channel_key]
            ch_dict = channel.__dict__.copy()
            ch_dict['color'] = COLORS[idx % len(COLORS)]
            data.append(ch_dict)
        await send_ws(json.dumps({
            "type": "channels",
            "data": data
        }))

    @routines.routine(delta=timedelta(seconds=10))
    async def fetch_rf_api(self):
        # try:
        #     await ws.ping()
        # except:
        #     return
            
        await send_ws(json.dumps({
            "type": "users",
            "data": [self.id_to_username[x] for x in self.ravenfall_users]
        }))
        
        while True:
            try:
                mult = await self.rf_api.get_global_mult()
                break
            except aiohttp.client_exceptions.ClientConnectorError:
                logging.error("Failed to connect to RavenNest! Retrying in 5s")
                await asyncio.sleep(5)
        mult_event = mult.event_name or ""
        await send_ws(json.dumps({
            "type": "multiplier",
            "multiplier": mult.multiplier,
            "event": mult_event,
            "start": mult.start_time.timestamp(),
            "end": mult.end_time.timestamp()
        }))
        self.current_mult = mult.multiplier
        group_size = 3
        users_grouped = [
            self.ravenfall_users[i:i+group_size] 
            for i in range(0, len(self.ravenfall_users), group_size)
        ]
        desync_samples = []
        desync_samples_per_channel: Dict[str, List[float]] = {}
        for user_id_group in users_grouped:
            out_data = []
            # results = await get_characters(self.rf_api, user_id)
            results = await asyncio.gather(
                *[get_characters(self.rf_api, a) for a in user_id_group]
            )
            result = [
                item 
                for sublist in results 
                for item in sublist
            ]
            now = datetime.now(timezone.utc) 
            
            for char in result:
                if char is None:
                    continue
                self.player_char_data[f"{char.user_name}_{char.index}"] = char
                progress = 0
                
                if char.training:
                    stat = char.training_stats[0]
                    progress = stat.level_exp / stat.total_exp_for_level
                training = ""
                if len(char.training_stats) == 1:
                    stat = char.training_stats[0]
                    training = f"{stat.skill.name} Lv. {stat.level}"
                elif len(char.training_stats) > 1:
                    stat_text = []
                    for stat in char.training_stats:
                        if stat.skill == Skills.Health:
                            stat_text_str = f"{langstuff.skill_contractions[stat.skill]} {char.hp}/{stat.level}"
                        else:
                            stat_text_str = f"{langstuff.skill_contractions[stat.skill]} {stat.level}"
                        stat_text.append(stat_text_str)
                    training = ', '.join(stat_text)
                else:
                    training = "Not training!"
                    
                where_island = ""
                if char.island:
                    where_island = f"at {char.island.name.capitalize()}"
                elif char.destination == Islands.Ferry:
                    where_island = f"on the ferry"
                else:
                    where_island = "sailing the seas"

                where = ""
                if char.in_raid:
                    where = "in a raid"
                if char.in_arena:
                    where = "in the arena"
                if char.in_dungeon:
                    where = "in a dungeon"
                
                rest = ""
                if char.in_onsen:
                    rest = "resting"
                if char.rested_time.total_seconds() == 2*60*60:
                    rest = "rested"

                captain = ""
                if char.is_captain:
                    captain = "as captain"

                destination = ""
                if char.waiting_for_ferry:
                    destination = f"waiting for ferry"

                target_item = ""
                if char.target_item:
                    item = char.target_item
                    target_item =  f"{item.amount}x {item.item.name}"
                    
                text1 = utils.capitalize_first_letter(utils.strjoin(' - ', training, target_item))
                text2 = utils.capitalize_first_letter(utils.strjoin(' ', where, where_island, destination, captain))
                channel_id = self.account_channels[char.twitch_id][char.index-1]
                if channel_id is not None:
                    channel_idx = 0
                    for idx, ch_id in enumerate(self.connected_channels.keys()):
                        if ch_id == channel_id:
                            channel_idx = idx
                            break
                    color = COLORS[channel_idx % len(COLORS)]
                    channel = await self.get_username(channel_id)
                    ...
                # if channel is not None:
                #     self.fetch_channels()
                name = char.name
                if char.name == str(char.index):
                    name = f"Character {char.index}"
                rest_progress = char.rested_time.total_seconds() / (2*60*60)
                
                status = ""
                char_is_offline = False
                desync_s = None
                if char.estimated_level_time and char.exp_per_hour > 0 and char.training_stats[0].level < 999:
                    training_time_server = char.estimated_level_time - now
                    closest_stat = char.training_stats[0]
                    exp_to_next_level = closest_stat.total_exp_for_level-closest_stat.level_exp
                    training_time_exp = timedelta(seconds=(exp_to_next_level) / (char.exp_per_hour/60/60))
                    train_time_diff = (training_time_exp - training_time_server)
                    desync_s = train_time_diff.total_seconds()
                    desync_samples.append(desync_s)
                    if not channel_id in desync_samples_per_channel:
                        desync_samples_per_channel[channel_id] = []
                    desync_samples_per_channel[channel_id].append(desync_s)
                    char_is_offline = train_time_diff.total_seconds() > 60*3  # 3 minutes
                if char_is_offline:
                    status = "offline"
                
                auto_statuses = []
                if char.auto_join_dungeon_count == math.inf:
                    auto_statuses.append("Auto dungeons")
                elif char.auto_join_dungeon_count > 0:
                    auto_statuses.append(f"{utils.pl(char.auto_join_dungeon_count, 'dungeons')}")
                    
                if char.auto_join_raid_count == math.inf:
                    auto_statuses.append("Auto raids")
                elif char.auto_join_raid_count > 0:
                    auto_statuses.append(f"{utils.pl(char.auto_join_raid_count, 'raids')}")

                if char.is_auto_resting and (char.auto_rest_start is not None):
                    if char.auto_rest_start != 0 or char.auto_rest_target != 120:
                        auto_statuses.append(
                            f"Auto resting from {char.auto_rest_start} min to {char.auto_rest_target} min"
                        )
                    else:
                        auto_statuses.append("Auto resting")

                combat_mult = 5
                train_time = ""
                now = datetime.now(timezone.utc)
                if char.estimated_level_time:
                    train_end_time = char.estimated_level_time
                else:
                    train_end_time = datetime(2000, 1, 1, tzinfo=timezone.utc)
                training_time_server = train_end_time - now
                if char.exp_per_hour > 0 and char.training:
                    # closest_stat = min(*char.training_stats, key=lambda x: x.total_exp_for_level-x.level_exp)
                    closest_stat = char.training_stats[0]
                    exp_to_next_level = closest_stat.total_exp_for_level-closest_stat.level_exp
                    training_time_exp = timedelta(seconds=(exp_to_next_level) / (char.exp_per_hour/60/60))
                else:
                    training_time_exp = timedelta(weeks=9999)
                s = utils.TimeSize.SMALL
                # train_time_format = utils.format_timedelta(training_time_server, s) + '/' + utils.format_timedelta(training_time_exp, s)
                train_time_diff = (training_time_exp - training_time_server)
                char_is_offline = train_time_diff.total_seconds() > 60*3  # 3 minutes
                if char.training in (Skills.Attack, Skills.Defense, Skills.Strength) and not (char.in_raid or char.in_dungeon):
                    training_time_exp /= combat_mult
                    training_time_server /= combat_mult
                # train_time_format = utils.format_timedelta(training_time_server, s)
                train_time_format = utils.format_timedelta(training_time_exp, s)
                if char.island and not char.in_onsen:
                    if char_is_offline:
                        train_time = f""
                    elif now < train_end_time:
                        if training_time_server.total_seconds() > 60*60*24*100:  # 99 days
                            train_time = f"Level in ∞"
                        else:
                            train_time = f"Level in {train_time_format}"
                    else:
                        train_time = f"Level in ---"
                        
                exp_per_hour = ""
                if train_time:
                    exp_per_hour = f"{char.exp_per_hour:,} exp/h"

                potion_statuses = []
                for status_effect in char.status_effects:
                    potion_statuses.append(
                        f"{langstuff.status_effect_names[status_effect.effect]} "\
                        f"+{status_effect.amount:.1%} for {utils.format_seconds(status_effect.time_left)}"
                    )

                rested_time = ""
                if char.rested_time.total_seconds() > 0:
                    rested_time = f"{utils.format_timedelta(char.rested_time)} rested"
                coins = f"{char.coins:,} coins"
                                
                rec_island = ""
                if char.training and not char.training == Skills.Sailing:
                    if char.training in (Skills.All, Skills.Health):
                        skill = max(char.attack, char.defense, char.strength, key=lambda x: x.level)
                    else:
                        skill = char.get_skill(char.training)
                        
                    is_training_combat = skill.skill in ravenpy.fighting_skills
                    recommended_island = ravenpy.get_island_for_level(skill.level)
                    if is_training_combat and skill.level < char.combat_level and char.combat_level < 300:
                        recommended_island = ravenpy.get_island_for_level(char.combat_level)
                    
                    if recommended_island != char.island:
                        rec_island = f"Sail to {recommended_island.name.capitalize()}"
                
                short_rec_armor = []
                rec_armor = ""
                rec_armor_mat = ravenpy.get_material_for_level(char.defense.level)
                eq = char.equipment
                armors = [
                    (eq.helmet, 'Helmet'),
                    (eq.chest, 'Chest'),
                    (eq.gloves, 'Gloves'),
                    (eq.leggings, 'Leggings'),
                    (eq.boots, 'Boots'),
                ]
                if not eq.weapon or eq.weapon.item.type in (ravenpy.ItemTypes.OneHandedSword, ravenpy.ItemTypes.OneHandedAxe):
                    armors.append((eq.shield, 'Shield'))
                for piece, short_l in armors:
                    if (not piece) or piece.item.material != rec_armor_mat:
                        in_inventory = char.get_item(f"{langstuff.material_names[rec_armor_mat]} {short_l}")
                        if in_inventory:
                            short_rec_armor.append("*")
                        else:
                            short_rec_armor.append(short_l[0])
                        rec_armor = f"{langstuff.material_names[rec_armor_mat]} set"
                    else:
                        short_rec_armor.append("-")
                if rec_armor:
                    rec_armor = utils.strjoin(" ", rec_armor, utils.strenclose("(", ")", "", utils.strjoin('',*short_rec_armor)))
                    has_armor_recs = True

                rec_weapon = ""
                if Skills.Health in (char.dungeon_combat_style, char.raid_combat_style) \
                or char.training in (Skills.All, Skills.Attack, Skills.Defense, Skills.Strength, Skills.Health) \
                or eq.weapon:
                    rec_weapon_mat = ravenpy.get_material_for_level(char.attack.level)
                    inv_check = []
                    if not eq.weapon:
                        rec_weapon = f"{rec_weapon_mat.name} weapon"
                        inv_check.append(f"{rec_weapon_mat.name} Sword")
                        inv_check.append(f"{rec_weapon_mat.name} 2H Sword")
                        inv_check.append(f"{rec_weapon_mat.name} Axe")
                        inv_check.append(f"{rec_weapon_mat.name} 2H Axe")
                    elif eq.weapon.item.material != rec_weapon_mat:
                        rec_weapon = f"{langstuff.material_names[rec_weapon_mat]} {utils.rm_words(eq.weapon.item.name, 1)}"
                        inv_check.append(rec_weapon)

                    for item_name in inv_check:
                        if char.get_item(item_name):
                            rec_weapon += "*"
                            break

                rec_staff = ""
                if Skills.Healing in (char.dungeon_combat_style, char.raid_combat_style, char.training)\
                or Skills.Magic in (char.dungeon_combat_style, char.raid_combat_style, char.training)\
                or eq.staff:
                    rec_mat = ravenpy.get_material_for_level(max(char.healing.level, char.magic.level))
                    if (not eq.staff) or eq.staff.item.material != rec_mat:
                        rec_staff = f"{langstuff.material_names[rec_mat]} staff"
                        if char.get_item(f"{langstuff.material_names[rec_mat]} Staff"):
                            rec_staff += "*"
                
                rec_bow = ""
                if Skills.Ranged in (char.dungeon_combat_style, char.raid_combat_style, char.training)\
                or eq.bow:
                    rec_mat = ravenpy.get_material_for_level(char.ranged.level)
                    if (not eq.bow) or eq.bow.item.material != rec_mat:
                        rec_bow = f"{langstuff.material_names[rec_mat]} bow"
                        if char.get_item(f"{langstuff.material_names[rec_mat]} Bow"):
                            rec_bow += "*"
                
                status_color = "#000000"
                if char.exp_per_hour == 0 and not char.in_onsen and not char.training == ravenpy.Skills.Sailing:
                    status_color = "#d62411"  # red
                if char.rested_time.total_seconds() == 2*60*60:
                    status_color = "#68aed4"  # blue
                if rec_island: 
                    status_color = "#ff8426"  # orange

                tooltip_text = utils.strjoin(
                    '\n\n',
                    utils.strjoin('\n',
                        text1 if len(text1) > 25 else "",
                        text2 if len(text2) > 25 else ""
                    ),
                    rec_island,
                    utils.strjoin('\n',
                        rec_armor,
                        rec_weapon,
                        rec_staff,
                        rec_bow,
                    ),
                    utils.strjoin('\n',
                        train_time,
                        exp_per_hour,
                        rested_time,
                    ),
                    utils.strjoin('\n', *auto_statuses),
                    utils.strjoin('\n', *potion_statuses),
                    coins,
                )

                aga = {
                    "id": char.char_id,
                    "name": name,
                    "idx": char.index,
                    "user": char.user_name,
                    "progress": progress,
                    "text1": text1,
                    "text2": text2,
                    "channel": channel,
                    "progress2": rest_progress,
                    "status": status,
                    "rest": rest,
                    "desync": desync_s,
                    "tooltip": tooltip_text,
                    "train_end_time": (now + training_time_exp).timestamp(),
                    "color": color,
                    "status_color": status_color
                    # "train_end_time": char.estimated_level_time.timestamp()
                }
                out_data.append(aga)
                
            await send_ws(json.dumps({
                "type": "update_chars",
                "data": out_data
            }))

        desync_samples.sort()
        sample_trim = int(round(len(desync_samples) * .2))
        if sample_trim > 0:
            desync_samples = desync_samples[sample_trim:-sample_trim]
        if len(desync_samples) == 0:
            desync_samples.append(0)
        avg_desync = sum(desync_samples) / len(desync_samples)
        
        self.desync_per_channel_id.clear()
        for channel_id, samples in desync_samples_per_channel.items():
            samples.sort()
            sample_trim = int(round(len(samples) * .2))
            if sample_trim > 0:
                samples = samples[sample_trim:-sample_trim]
            if len(samples) == 0:
                samples.append(0)
            channel_avg_desync = sum(samples) / len(samples)
            self.desync_per_channel_id[channel_id] = channel_avg_desync
        # with open("desync.csv", "a") as f:
        #     f.write(f"{time.time()},{avg_desync}\n")
        await send_ws(json.dumps({
            "type": "update_desync",
            "seconds": avg_desync
        }))
    
    @routines.routine(delta=timedelta(minutes=15), wait_first=True)
    async def resync_routine(self):
        for channel_id, desync in self.desync_per_channel_id.items():
            if not self.user_info.get(channel_id, {}).get("can_add_moderators", False):
                continue
            user_name = self.user_info.get(channel_id, {}).get("name", "")
            if abs(desync) > 60*3:  # 3 minutes
                await self.handle_random_relog(channel_id, user_name)
            
   

rfapi: ravenpy.RavenNest
twitch_client: ChatClient
async def main() -> None:
    # Setup logging, this is optional, however a nice to have...
    twitchio.utils.setup_logging(level=logging.INFO)
    rfapi = ravenpy.RavenNest(os.getenv("API_USER"), os.getenv("API_PASS"))
    await rfapi.login()

    async def runner() -> None:
        global twitch_client
        async with ChatClient(rfapi) as bot:
            twitch_client = bot
            await bot.start()

    try:
        await runner()
    except KeyboardInterrupt:
        LOGGER.warning("Shutting down due to Keyboard Interrupt...")

asyncio.run(main())