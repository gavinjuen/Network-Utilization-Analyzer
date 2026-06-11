import pandas as pd
from django.core.management.base import BaseCommand
from dashboard.models import RingNodeMapping
from dashboard.calculations import normalize_ring_name, normalize_board_pair


class Command(BaseCommand):
    help = "Import fixed ring node mapping Excel"

    def add_arguments(self, parser):
        parser.add_argument("excel_file", type=str)

    def handle(self, *args, **options):
        excel_file = options["excel_file"]

        df = pd.read_excel(excel_file)
        df.columns = df.columns.str.strip()

        required = ["Link Group Name", "Link Group Description", "Source NE", "Sink NE"]
        missing = [c for c in required if c not in df.columns]

        if missing:
            self.stderr.write(f"Missing columns: {missing}")
            return

        RingNodeMapping.objects.all().delete()

        objects = []

        for _, row in df.iterrows():
            link_group_name = str(row["Link Group Name"]).strip()
            board_pair = str(row["Link Group Description"]).strip()
            source_ne = str(row["Source NE"]).strip()
            sink_ne = str(row["Sink NE"]).strip()

            if not link_group_name or link_group_name.lower() == "nan":
                continue

            objects.append(RingNodeMapping(
                link_group_name=link_group_name,
                normalized_ring=normalize_ring_name(link_group_name),
                board_pair=board_pair,
                normalized_board_pair=normalize_board_pair(board_pair),
                source_ne=source_ne,
                sink_ne=sink_ne,
            ))

        RingNodeMapping.objects.bulk_create(objects, batch_size=1000)

        self.stdout.write(
            self.style.SUCCESS(f"Imported {len(objects)} ring node mappings.")
        )