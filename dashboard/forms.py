from django import forms

class MultiFileInput(forms.ClearableFileInput):
    allow_multiple_selected = True

class MultiFileField(forms.FileField):
    widget = MultiFileInput

    def clean(self, data, initial=None):
        if not data:
            raise forms.ValidationError("Please select at least one file.")

        if not isinstance(data, (list, tuple)):
            data = [data]

        cleaned_files = []
        errors = []

        for item in data:
            try:
                cleaned_files.append(super().clean(item, initial))
            except forms.ValidationError as exc:
                errors.extend(exc.messages)

        if errors:
            raise forms.ValidationError(errors)

        return cleaned_files

class UploadFilesForm(forms.Form):
    files = MultiFileField(
        help_text="Upload one or more CSV or ZIP files."
    )
    skiprows = forms.IntegerField(
        min_value=0,
        max_value=30,
        initial=6,
        help_text="Rows to skip before actual table header.",
    )
