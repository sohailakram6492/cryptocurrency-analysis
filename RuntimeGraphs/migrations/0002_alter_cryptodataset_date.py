# Generated by Django 3.2.9 on 2021-11-24 08:36

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('RuntimeGraphs', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='cryptodataset',
            name='date',
            field=models.TimeField(),
        ),
    ]