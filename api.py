import requests
from discord import Webhook, RequestsWebhookAdapter
from config import DC_URL
import discord


def f(json_data):
    requests.post("https://f.implements.io/api/post/create", data=json_data)


def dc(path):
    Webhook.from_url(DC_URL, adapter=RequestsWebhookAdapter()).send(file=discord.File(path))


if __name__ == '__main__':
    dc()
