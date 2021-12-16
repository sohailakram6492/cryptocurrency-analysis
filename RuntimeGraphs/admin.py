from django.contrib import admin
from django.contrib.admin.decorators import register
from .models import CryptoDataset, ETHDataset, Wallet, Purchase
from django.utils.html import mark_safe

class DatasetAdmin(admin.ModelAdmin):
    list_display = ('date', 'open', 'high', 'low', 'close', 'volume')

admin.site.register(CryptoDataset)

class WalletAdmin(admin.ModelAdmin):
    list_display = ('Id', 'wallet')

admin.site.register(Wallet)


class PurchaseAdmin(admin.ModelAdmin):
    list_display = ('currency_name', 'currency_purchase', 'currency_purchase', 'is_sold', 'net_gain')

from .models import register

admin.site.register(Purchase)
admin.site.register(ETHDataset)

admin.site.register(register)
