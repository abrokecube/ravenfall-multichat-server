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

async def handle_s

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
    
    async def get_username(self, user_id: str):
        user_id = str(user_id)
        if not user_id in self.username_to_id:
            user = await self.fetch_users(ids=[user_id])
            self.username_to_id[user[0].name] = user_id
            self.id_to_username[user_id] = user[0].display_name
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
            "can_add_moderators": False
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
    
    async def handle_sail(self, channel_name: str):
        ...

    async def process_commands(self, payload: twitchio.ChatMessage):
        prefix = "?"
        if len(payload.text) < len(prefix) + 1 or payload.text[0] != "?":
            return
        spl = payload.text[len(prefix):].split()
        command = spl[0].lower()
        args = spl[1:]
        match command:
            case "sail":
                await handle_sail(payload.broadcaster.name)

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
        args = text.lstrip("!>").split()
        command = args[0]
        args = args[1:]
        match command:
            case "join":
                if len(args) < 1:
                    return
                if not args[0].isdigit():
                    return
                index = int(args[0])
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

    @routines.routine(delta=timedelta(seconds=6))
    async def fetch_rf_api(self):
        try:
            await ws.ping()
        except:
            return
            
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
        group_size = 3
        users_grouped = [
            self.ravenfall_users[i:i+group_size] 
            for i in range(0, len(self.ravenfall_users), group_size)
        ]
        desync_samples = []
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
                channel = self.account_channels[char.twitch_id][char.index-1]
                if channel is not None:
                    channel_idx = 0
                    for idx, ch_id in enumerate(self.connected_channels.keys()):
                        if ch_id == channel:
                            channel_idx = idx
                            break
                    color = COLORS[channel_idx % len(COLORS)]
                    channel = await self.get_username(channel)
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
        # with open("desync.csv", "a") as f:
        #     f.write(f"{time.time()},{avg_desync}\n")
        await send_ws(json.dumps({
            "type": "update_desync",
            "seconds": avg_desync
        }))
   

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