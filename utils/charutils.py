from typing import List, Tuple, Iterable
from twitchio.ext import commands
import ravenpy
import asyncio
from async_lru import alru_cache
import re
from .utils import split_arguments, SplitQuery, SplitWildcard, strjoin, is_twitch_username
DEBUG = True

def match_identifier(chars: List[ravenpy.Character], target: str):
    for idx, char in enumerate(chars):
        char_id = set()   
        char_id.add(str(idx+1))
        if idx == 0:
            char_id.add(str(0))
        char_id.add(char.identifier.lower())
        
        if target.lower() in char_id:
            return char
    return None

@alru_cache(ttl=3)
async def _get_characters(bot: commands.Bot, rfapi: ravenpy.RavenNest, *, user_id: str=None, user_name=None):
    uid = user_id
    if uid is None:
        if user_name:
            user_query = await bot.fetch_users(logins=[user_name.strip('@'),])
            if user_query:
                user_id = user_query[0].id
            else:
                return None
        else:
            return None
    tasks = [
        rfapi.get_character(user_id,'1'),
        rfapi.get_character(user_id,'2'),
        rfapi.get_character(user_id,'3')
    ]
    user_chars = await asyncio.gather(*tasks, return_exceptions=not DEBUG)
    user_chars = [x for x in user_chars if isinstance(x, ravenpy.Character)]
    return user_chars

async def dummy(returns=None):
    return returns

class CharSearchResult:
    def __init__(self, chars: Iterable[ravenpy.Character], leftover: str):
        char_tuple = tuple(chars)
        self.chars = char_tuple
        self.characters = char_tuple
        self.leftover_args = leftover.split()

async def get_user_characters(rfapi: ravenpy.RavenNest, ctx: commands.Context, user: str=''):
    if user and not is_twitch_username(user):
        await ctx.reply(f"uuh {user} is not a valid username.")
        return None
    if not user:
        user_chars = await _get_characters(ctx.bot, rfapi, user_id=ctx.author.id)
        if user_chars is None:
            await ctx.reply(f"uuh You dont exist??? om")
            return None
        elif len(user_chars) == 0:
            await ctx.reply(f"YEP You have no characters.")
            return None
        else:
            return user_chars
    else:
        user_chars = await _get_characters(ctx.bot, rfapi, user_name=user)
        if user_chars is None:
            await ctx.reply(f"YEP User '{user}' not found.")
            return None
        elif len(user_chars) == 0:
            await ctx.reply(f"YEP User '{user}' has no characters.")
            return None
        else:
            return user_chars


async def search_user_characters(
    rfapi: ravenpy.RavenNest,
    ctx: commands.Context,
    *args: str,
    single_char_only=False,
    author_chars_fallback=False,
    all_chars_fallback=False,
) -> CharSearchResult | None:
    include_user = False
    args_filtered = [x for x in args if x]
    if args_filtered and is_twitch_username(args_filtered[0]):
        include_user = True
        
    tasks = [
        _get_characters(ctx.bot, rfapi, user_id=ctx.author.id),
        _get_characters(ctx.bot, rfapi, user_name=args_filtered[0]) if include_user else dummy(),
    ]
            
    author_chars, user_chars = await asyncio.gather(*tasks, return_exceptions=not DEBUG)
    author_char_names = [x.name for x in author_chars]
    user_char_names = [x.name for x in user_chars] if user_chars is not None else []
    
    if not args_filtered:
        if not author_chars:
            await ctx.reply(f"uuh You have no characters.")
            return None
        if len(author_chars) > 1 and single_char_only:
            await ctx.reply(f"uuh Please specify a character name or index. " \
            f"You have {strjoin(', ', *author_char_names, before_end=' and ')}.")
            return None
        return CharSearchResult(author_chars, '')

    username = ''
    if user_chars is not None:
        username = args_filtered[0]
    # char_indexes = [str(x) for x in range(max(len(author_chars), len(user_chars) if user_chars else 0))]
    user_char_indexes = [str(x.index) for x in user_chars] if user_chars else []
    author_char_indexes = [str(x.index) for x in author_chars]
    if username:
        result = split_arguments(args_filtered,
            SplitWildcard(),
            SplitQuery(user_char_names+author_char_names, optional=True),
            SplitQuery(user_char_indexes+author_char_indexes, optional=True),
            SplitWildcard()
        )
        user_q, char_q, index_q, rest_q = [x.text for x in result]
        rest_q_split = rest_q.split()
        if rest_q_split and rest_q_split[0] == username:
            user_q = username.lstrip('@')
            rest_q = " ".join(rest_q_split[1:])
    else:
        char_indexes = [str(x.index) for x in author_chars]
        result = split_arguments(args_filtered, 
            SplitQuery(char_indexes, optional=True), 
            SplitQuery(author_char_names, optional=True), 
            SplitWildcard()
        )
        user_q = ''
        index_q, char_q, rest_q = [x.text for x in result]
    out_chars = []
    
    if author_chars_fallback and not user_chars:
        rest_q = strjoin(' ', username, rest_q)
        user_q = ''
        
    if user_q:
        if not user_chars:
            await ctx.reply(
                f"YEP '{username}' has no characters."
            )
            return None
        elif char_q:
            out_chars = [x for x in user_chars if x.name == char_q]
        elif index_q:
            out_chars = [x for x in user_chars if x.index == int(index_q)]
            if not out_chars:
                await ctx.reply(
                    f"uuh User {username} doesn't have Character {index_q}. " \
                    f"They do have {strjoin(', ', *user_char_names, before_end=' and ')}."
                )
                return None
        elif all_chars_fallback:
            out_chars = user_chars
        elif rest_q:
            await ctx.reply(
                f"uuh No character by {username} named '{rest_q}'. " \
                f"They do have {strjoin(', ', *user_char_names, before_end=' and ')}."
            )
            return None
        elif len(user_chars) > 1 and single_char_only:
            await ctx.reply(f"uuh Please specify a character name or index. " \
                f"{username} has {strjoin(', ', *user_char_names, before_end=' and ')}."
            )
            return None
        else:
            out_chars = user_chars
    else:
        if not author_chars:
            await ctx.reply(
                f"YEP You have no characters."
            )
            return None
        elif char_q:
            out_chars = [x for x in author_chars if x.name == char_q]
        elif index_q:
            out_chars = [x for x in author_chars if x.index == int(index_q)]
            if not out_chars:
                await ctx.reply(
                    f"uuh You don't have Character {index_q}. " \
                    f"You do have {strjoin(', ', *author_char_names, before_end=' and ')}."
                )
                return None
        elif all_chars_fallback:
            out_chars = author_chars
        elif rest_q:
            await ctx.reply(
                f"uuh You don't have a character named '{rest_q}'. " \
                f"You have {strjoin(', ', *author_char_names, before_end=' and ')}."
            )
            return None
        elif len(author_chars) > 1 and single_char_only:
            await ctx.reply(f"uuh Please specify a character name or index. " \
                f"You have {strjoin(', ', *author_char_names, before_end=' and ')}."
            )
            return None
        else:
            out_chars = author_chars
    if single_char_only and len(out_chars) != 1:
        assert(False)
    if len(out_chars) == 0:
        await ctx.reply("uuh Ravenfall servers may not be responding.")
        return 
    return CharSearchResult(out_chars, rest_q)
    


