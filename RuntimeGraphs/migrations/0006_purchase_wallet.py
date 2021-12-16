# Generated by Django 3.2.9 on 2021-11-25 09:13

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('RuntimeGraphs', '0005_auto_20211124_1016'),
    ]

    operations = [
        migrations.CreateModel(
            name='Purchase',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date', models.DateTimeField()),
                ('currency_name', models.CharField(max_length=255)),
                ('currency_purchase', models.FloatField()),
                ('is_sold', models.BooleanField()),
                ('net_gain', models.FloatField()),
            ],
        ),
        migrations.CreateModel(
            name='Wallet',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('wallet', models.FloatField()),
            ],
        ),
    ]