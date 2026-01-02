import socket
import threading
import time
import ipywidgets as widgets
from IPython.display import display
from coordinator import Coordinator


class CoordinatorGUI:
    """
    GUI wrapper for the coordinator using Jupyter widgets.
    Extracted from the original notebook for better separation of concerns.
    """
    
    def __init__(self, host='0.0.0.0', port=5000, lease_duration=5.0):
        self.coordinator = Coordinator(host, port, lease_duration)
        
        # GUI widgets
        self.node_widgets_cache = {}
        self.dashboard_container = widgets.GridBox(
            layout=widgets.Layout(grid_template_columns="repeat(4, 150px)", gap="10px")
        )
        self.header_widget = widgets.HTML("<h3>Coordinator</h3>")
        self.info_widget = widgets.HTML("Initialisiere...")
        self.stop_button = widgets.Button(
            description="Server Stoppen",
            button_style='danger',
            icon='power-off'
        )
        
        # GUI update thread
        self.gui_thread = None
        self.stop_button.on_click(self._trigger_stop)
    
    def start(self):
        """Start coordinator and GUI."""
        self.coordinator.start()
        
        # Start GUI update thread
        self.gui_thread = threading.Thread(target=self._update_dashboard, daemon=True)
        self.gui_thread.start()
        
        # Create and display UI
        ui = widgets.VBox([
            self.header_widget,
            self.info_widget,
            self.stop_button,
            widgets.HTML("<hr>"),
            self.dashboard_container
        ])
        display(ui)
    
    def stop(self):
        """Stop coordinator and GUI."""
        self.coordinator.stop()
    
    def _get_node_ui(self, nid):
        """Create UI widgets for a node - extracted from notebook."""
        lbl = widgets.HTML(
            value=f"<b>{nid}</b><br>IDLE",
            layout=widgets.Layout(height='40px', justify_content='center')
        )
        prog = widgets.FloatProgress(
            value=0.0,
            min=0.0,
            max=self.coordinator.lease_duration,
            bar_style='info',
            layout=widgets.Layout(width='100%', height='20px')
        )
        box = widgets.VBox(
            [lbl, prog],
            layout=widgets.Layout(
                border='1px solid #ccc',
                padding='10px',
                align_items='center'
            )
        )
        return {'box': box, 'label': lbl, 'prog': prog}
    
    def _update_dashboard(self):
        """Dashboard update loop - extracted from notebook."""
        while self.coordinator.running:
            state = self.coordinator.get_state()
            
            # Update info widget
            status_text = "ONLINE" if state['running'] else "GESTOPPT"
            my_ip = socket.gethostbyname(socket.gethostname())
            self.info_widget.value = f"IP: {my_ip} | Port: {self.coordinator.port} | Status: {status_text}"
            
            # Update node widgets
            active_children = []
            waiting_ids = [x[0] for x in state['queue']]
            
            for nid in sorted(state['known_nodes']):
                if nid not in self.node_widgets_cache:
                    self.node_widgets_cache[nid] = self._get_node_ui(nid)
                
                ui = self.node_widgets_cache[nid]
                active_children.append(ui['box'])
                
                if nid == state['current_holder']:
                    # Current token holder
                    rem = state['lease_remaining']
                    ui['label'].value = f"<b style='color:#27ae60'>{nid}</b><br>RUNNING"
                    ui['prog'].value = rem
                    ui['prog'].bar_style = 'success'
                    ui['box'].layout.border = '2px solid #27ae60'
                elif nid in waiting_ids:
                    # In queue waiting
                    pos = waiting_ids.index(nid) + 1
                    ui['label'].value = f"<b>{nid}</b><br>WAITING ({pos})"
                    ui['prog'].value = self.coordinator.lease_duration
                    ui['prog'].bar_style = 'warning'
                    ui['box'].layout.border = '2px solid #f39c12'
                else:
                    # Idle
                    ui['label'].value = f"<b>{nid}</b><br>IDLE"
                    ui['prog'].value = 0
                    ui['prog'].bar_style = ''
                    ui['box'].layout.border = '1px solid #ccc'
            
            # Update dashboard if children changed
            if tuple(active_children) != self.dashboard_container.children:
                self.dashboard_container.children = tuple(active_children)
            
            time.sleep(0.1)
        
        self.info_widget.value = "<b>SYSTEM HERUNTERGEFAHREN.</b> Socket geschlossen."
    
    def _trigger_stop(self, button):
        """Stop button click handler - extracted from notebook."""
        if not self.coordinator.running:
            return
        
        print("Beende System...")
        self.stop_button.disabled = True
        self.stop_button.description = "Stoppe..."
        
        self.coordinator.stop()


def start_coordinator_gui(host='0.0.0.0', port=5000, lease_duration=5.0):
    """Convenience function to start coordinator with GUI."""
    gui = CoordinatorGUI(host, port, lease_duration)
    gui.start()
    return gui