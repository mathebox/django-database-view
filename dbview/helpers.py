from django.db import migrations, connection
from django.apps import apps


class CreateView(migrations.CreateModel):
    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.name)

        if not self.allow_migrate_model(schema_editor.connection.alias, model):
            raise
        models = apps.get_app_config(app_label).models_module
        model = getattr(models, self.name)

        if hasattr(model, 'view'):
            qs = str(model.view())
        else:
            raise Exception('Your view needs to define either view or ' +
                            'get_view_str')

        query_params = {
            'view_name': model._meta.db_table,
            'definition': qs,
        }

        self.drop_view(query_params)

        sql = 'CREATE VIEW %(view_name)s AS %(definition)s'
        schema_editor.execute(sql % query_params, None)

    def database_backwards(self, app_label, schema_editor, from_state, to):
        model = from_state.apps.get_model(app_label, self.name)
        query_params = {
            'view_name': schema_editor.quote_name(model._meta.db_table),
        }
        self.drop_view(query_params)

    def drop_view(self, query_params):
        cursor = connection.cursor()
        check_sql = """
            SELECT COUNT(*)
            FROM views
            WHERE schema_name=current_schema AND view_name='%s'
        """
        cursor.execute(check_sql % query_params['view_name'].upper(), None)

        # Drop view if needed
        drop_view = cursor.fetchone()[0] != 0
        if drop_view:
            sql = 'DROP VIEW %(view_name)s'
            cursor.execute(sql % query_params, None)
