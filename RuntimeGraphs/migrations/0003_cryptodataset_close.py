# Generated by Django 3.2.9 on 2021-11-24 08:44

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('RuntimeGraphs', '0002_alter_cryptodataset_date'),
    ]

    operations = [
        migrations.AddField(
            model_name='cryptodataset',
            name='close',
            field=models.FloatField(default=0),
            preserve_default=False,
        ),
    ]
