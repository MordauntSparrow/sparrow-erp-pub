import os
import importlib

# A list to keep track of loaded plugins
loaded_plugins = []

# This function will register all available plugins
def register_plugins(app):
    # Correct the path to the plugins directory
    plugins_dir = os.path.dirname(__file__)  # This gives the current directory: app/plugins

    # Loop through each folder/plugin in the plugins directory
    for plugin_name in os.listdir(plugins_dir):
        plugin_path = os.path.join(plugins_dir, plugin_name)

        # If it's a directory (i.e., plugin), try to import it
        if os.path.isdir(plugin_path):
            try:
                # Dynamically import the plugin
                plugin_module = importlib.import_module(f'app.plugins.{plugin_name}.routes')
                
                # Call the register function to initialize the plugin
                if hasattr(plugin_module, 'get_public_blueprint'):
                    plugin_module.register(app)
                    app.logger.info(f'Plugin {plugin_name} loaded successfully.')
                    loaded_plugins.append(plugin_name)  # Keep track of successfully loaded plugins
                else:
                    app.logger.warning(f'Plugin {plugin_name} does not have a register function.')

            except Exception as e:
                app.logger.error(f'Failed to load plugin {plugin_name}: {e}')

# This function will return the list of successfully loaded plugins
def get_loaded_plugins():
    return loaded_plugins
