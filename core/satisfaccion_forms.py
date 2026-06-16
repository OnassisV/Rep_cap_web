"""Formularios para carga de satisfacción."""

from django import forms


class CargarSatisfaccionExcelForm(forms.Form):
    """Formulario para cargar Excel de satisfacción."""

    archivo_excel = forms.FileField(
        label='Archivo Excel',
        help_text='Selecciona archivo Excel con datos de satisfacción (.xlsx)',
        widget=forms.FileInput(attrs={
            'accept': '.xlsx,.xls',
            'class': 'form-control',
        }),
    )

    reemplazar = forms.BooleanField(
        label='Reemplazar datos existentes por código',
        required=False,
        help_text='Si está marcado, elimina registros anteriores del mismo código antes de insertar',
        widget=forms.CheckboxInput(attrs={
            'class': 'form-check-input',
        }),
    )

    def clean_archivo_excel(self):
        archivo = self.cleaned_data['archivo_excel']

        # Validar extensión
        if not archivo.name.lower().endswith(('.xlsx', '.xls')):
            raise forms.ValidationError('El archivo debe ser Excel (.xlsx o .xls)')

        # Validar tamaño (máx 10 MB)
        if archivo.size > 10 * 1024 * 1024:
            raise forms.ValidationError('El archivo no puede exceder 10 MB')

        return archivo
