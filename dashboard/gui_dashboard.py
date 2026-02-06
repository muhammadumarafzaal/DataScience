"""
FORENSIC DESKTOP TELEMETRY SUITE (Phase 10)
=========================================

This module implements a professional Tkinter-based desktop interface 
for the local exploration of NYC Congestion Pricing Audit artifacts.

Features:
- Composite KPI Grid: Real-time adherence and treasury metrics.
- Multi-Tabular Forensic Views: High-resolution visual analysis.
- Elasticity & Leakage Telemetry: Investigative data depth.
- Performance Architecture: Buffered image loading and scrollable canvases.
"""

import tkinter as tk
from tkinter import ttk, Canvas, Scrollbar
from PIL import Image, ImageTk
import duckdb
from pathlib import Path
import sys

# Workspace Integration
sys.path.append(str(Path(__file__).parent.parent))

from src.settings import DATAMART_DIR, CHART_EXPORTS_DIR


class AuditForensicDashboard:
    """
    NYC Congestion Pricing Forensic Audit - Desktop Telemetry Suite
    """
    
    def __init__(self, root_window):
        self.root = root_window
        self.root.title("NYC Forensic Transit Audit | Desktop Intelligence Suite")
        
        # Dynamic Scaling & Centering
        viewport_w = int(root_window.winfo_screenwidth() * 0.85)
        viewport_h = int(root_window.winfo_screenheight() * 0.85)
        start_x = (root_window.winfo_screenwidth() - viewport_w) // 2
        start_y = (root_window.winfo_screenheight() - viewport_h) // 2
        
        self.root.geometry(f"{viewport_w}x{viewport_h}+{start_x}+{start_y}")
        self.root.minsize(1280, 720)
        
        # Color Palette - Professional Slate & Indigo
        self.palette = {
            'deep_slate': '#0F172A',
            'card_slate': '#1E293B',
            'indigo_prime': '#6366F1',
            'emerald_safe': '#10B981',
            'crimson_leak': '#EF4444',
            'bone_white': '#F8FAFC',
            'muted_slate': '#94A3B8',
            'tab_active': '#334155'
        }
        
        self.initialize_ux_architecture()
        self.construct_system_header()
        
        # Primary Navigation Module
        self.nav_tabs = ttk.Notebook(root_window, style='Audit.TNotebook')
        self.nav_tabs.pack(fill='both', expand=True, padx=25, pady=20)
        
        # Tab Initialization
        self.initialize_synthesis_tab()
        self.initialize_temporal_tab()
        self.initialize_fiscal_tab()
        self.initialize_spatial_tab()
        self.initialize_leakage_tab()
        
        self.construct_system_footer()
    
    def initialize_ux_architecture(self):
        """Configures the structural and visual theme of the dashboard."""
        ux_style = ttk.Style()
        ux_style.theme_use('clam')
        
        ux_style.configure('Audit.TNotebook', background=self.palette['deep_slate'], borderwidth=0)
        ux_style.configure('Audit.TNotebook.Tab', 
                         padding=[28, 14], 
                         font=('Outfit', 11, 'bold'),
                         background=self.palette['tab_active'],
                         foreground=self.palette['muted_slate'])
        ux_style.map('Audit.TNotebook.Tab',
                   background=[('selected', self.palette['indigo_prime'])],
                   foreground=[('selected', 'white')])
    
    def construct_system_header(self):
        """Architects the institutional header for the dashboard."""
        header_shell = tk.Frame(self.root, bg=self.palette['deep_slate'], height=130)
        header_shell.pack(fill='x')
        header_shell.pack_propagate(False)
        
        tk.Label(
            header_shell,
            text="NYC FORENSIC TRANSIT AUDIT",
            font=('Outfit', 36, 'bold'),
            bg=self.palette['deep_slate'],
            fg='white'
        ).pack(pady=(25, 0))
        
        tk.Label(
            header_shell,
            text="Principal Investigator: Muhammad Umar Afzaal | Institutional ID: 23F-3106",
            font=('Outfit', 12),
            bg=self.palette['deep_slate'],
            fg=self.palette['indigo_prime']
        ).pack()
    
    def construct_system_footer(self):
        """Architects the persistence footer."""
        footer_shell = tk.Frame(self.root, bg=self.palette['deep_slate'], height=45)
        footer_shell.pack(fill='x', side='bottom')
        footer_shell.pack_propagate(False)
        
        tk.Label(
            footer_shell,
            text="Big Data Architecture: DuckDB Kernel | Visualization Engine: Matplotlib | Environment: Production 2025",
            font=('Outfit', 9),
            bg=self.palette['deep_slate'],
            fg=self.palette['muted_slate']
        ).pack(pady=10)
    
    def initialize_synthesis_tab(self):
        """Constructs the executive synthesis tab with real-time KPI telemetry."""
        synthesis_frame = tk.Frame(self.nav_tabs, bg=self.palette['deep_slate'])
        self.nav_tabs.add(synthesis_frame, text="💎 Synthesis")
        
        tk.Label(
            synthesis_frame, 
            text="INSTITUTIONAL KPI TELEMETRY", 
            font=('Outfit', 24, 'bold'),
            bg=self.palette['deep_slate'],
            fg='white'
        ).pack(pady=(40, 25))
        
        try:
            db_engine = duckdb.connect()
            artifact_path = DATAMART_DIR / "trips_by_zone_category.parquet"
            
            if artifact_path.exists():
                kpi_data = db_engine.execute(f"""
                    SELECT 
                        SUM(trip_count) as vol,
                        SUM(total_congestion_collected) as rev,
                        SUM(trips_with_surcharge) as cmp,
                        SUM(trips_without_surcharge) as lkg
                    FROM read_parquet('{artifact_path}')
                    WHERE after_congestion_start = 1
                """).fetchone()
                
                vol, rev, cmp, lkg = kpi_data
                vol = vol or 0; rev = rev or 0; cmp = cmp or 0; lkg = lkg or 0
                
                adherence = (cmp / (cmp + lkg) * 100) if (cmp + lkg) > 0 else 0
                leakage = 100 - adherence
                
                # Composite Grid
                grid_shell = tk.Frame(synthesis_frame, bg=self.palette['deep_slate'])
                grid_shell.pack(pady=20, fill='both', expand=True, padx=50)
                
                for i in range(2):
                    grid_shell.grid_rowconfigure(i, weight=1)
                    grid_shell.grid_columnconfigure(i, weight=1)
                
                self.construct_forensic_kpi_card(grid_shell, "DATA CAPACITY", f"{vol:,.0f}", self.palette['indigo_prime'], 0, 0)
                self.construct_forensic_kpi_card(grid_shell, "GROSS TREASURY", f"${rev:,.2f}", self.palette['emerald_safe'], 0, 1)
                self.construct_forensic_kpi_card(grid_shell, "POLICY ADHERENCE", f"{adherence:.2f}%", self.palette['indigo_prime'], 1, 0)
                self.construct_forensic_kpi_card(grid_shell, "REVENUE LEAKAGE", f"{leakage:.2f}%", self.palette['crimson_leak'], 1, 1)
                
            db_engine.close()
        except Exception as kpi_err:
            tk.Label(synthesis_frame, text=f"Telemetry Offline: {kpi_err}", fg=self.palette['crimson_leak'], bg=self.palette['deep_slate']).pack()

    def construct_forensic_kpi_card(self, parent, label, data, accent, r, c):
        """Constructs a modern glassmorphic-inspired card for KPI display."""
        shell = tk.Frame(parent, bg=self.palette['deep_slate'])
        shell.grid(row=r, column=c, padx=20, pady=20, sticky='nsew')
        
        card = tk.Frame(shell, bg=self.palette['card_slate'], highlightthickness=1, highlightbackground=accent)
        card.pack(fill='both', expand=True)
        
        tk.Label(card, text=label, font=('Outfit', 13, 'bold'), bg=self.palette['card_slate'], fg=self.palette['muted_slate']).pack(pady=(30, 5))
        tk.Label(card, text=data, font=('Outfit Semibold', 38), bg=self.palette['card_slate'], fg=accent).pack(pady=(0, 30))

    def initialize_temporal_tab(self):
        """Renders the temporal flux visualization."""
        tab = ttk.Frame(self.nav_tabs)
        self.nav_tabs.add(tab, text="🌀 Temporal")
        self.render_visual_canvas(tab, CHART_EXPORTS_DIR / "temporal_volume_dynamics.png", "LONGITUDINAL TRANSIT OSCILLATIONS")

    def initialize_fiscal_tab(self):
        """Renders the fiscal forensics visualization."""
        tab = ttk.Frame(self.nav_tabs)
        self.nav_tabs.add(tab, text="⚡ Fiscal")
        self.render_visual_canvas(tab, CHART_EXPORTS_DIR / "fiscal_trajectory_mapping.png", "SURCHARGE CAPTURE DYNAMICS")

    def initialize_spatial_tab(self):
        """Renders the spatial invariance visualization."""
        tab = ttk.Frame(self.nav_tabs)
        self.nav_tabs.add(tab, text="🛰️ Spatial")
        self.render_visual_canvas(tab, CHART_EXPORTS_DIR / "spatial_load_distribution.png", "CBD INTERSECTION CATEGORY DISPERSION")

    def initialize_leakage_tab(self):
        """Renders the compliance telemetry visualization."""
        tab = ttk.Frame(self.nav_tabs)
        self.nav_tabs.add(tab, text="🔍 Leakage")
        self.render_visual_canvas(tab, CHART_EXPORTS_DIR / "compliance_leakage_forensics.png", "REVENUE LEAKAGE FORENSIC ANALYSIS")

    def render_visual_canvas(self, host_frame, artifact_path, nomenclature):
        """Standardized rendering engine for visual artifacts with scrolling."""
        container = tk.Frame(host_frame, bg=self.palette['deep_slate'])
        container.pack(fill='both', expand=True)
        
        tk.Label(container, text=nomenclature, font=('Outfit', 20, 'bold'), bg=self.palette['deep_slate'], fg='white').pack(pady=20)
        
        image_shell = tk.Frame(container, bg=self.palette['deep_slate'])
        image_shell.pack(fill='both', expand=True, padx=30, pady=10)
        
        try:
            if artifact_path.exists():
                raw_img = Image.open(artifact_path)
                
                # Adaptive Scaling
                bound_w = 1200; bound_h = 650
                w, h = raw_img.size
                
                if w > bound_w or h > bound_h:
                    ratio = min(bound_w / w, bound_h / h)
                    img_ready = raw_img.resize((int(w * ratio), int(h * ratio)), Image.Resampling.LANCZOS)
                else:
                    img_ready = raw_img
                
                photo = ImageTk.PhotoImage(img_ready)
                
                canvas = Canvas(image_shell, bg=self.palette['deep_slate'], highlightthickness=0)
                v_scroller = Scrollbar(image_shell, orient='vertical', command=canvas.yview)
                h_scroller = Scrollbar(image_shell, orient='horizontal', command=canvas.xview)
                
                canvas.configure(yscrollcommand=v_scroller.set, xscrollcommand=h_scroller.set)
                
                v_scroller.pack(side='right', fill='y')
                h_scroller.pack(side='bottom', fill='x')
                canvas.pack(side='left', fill='both', expand=True)
                
                canvas.create_image(0, 0, anchor='nw', image=photo)
                canvas.img_ref = photo
                canvas.configure(scrollregion=canvas.bbox('all'))
            else:
                tk.Label(container, text="Artifact Missing: Initialize Forensic Pipeline", fg=self.palette['crimson_leak'], bg=self.palette['deep_slate'], font=('Outfit', 16)).pack(pady=100)
        except Exception as img_err:
            tk.Label(container, text=f"Rendering Failure: {img_err}", fg=self.palette['crimson_leak'], bg=self.palette['deep_slate']).pack(pady=100)


def launch_forensic_dashboard():
    """System entry point for the desktop suite."""
    root = tk.Tk()
    engine = AuditForensicDashboard(root)
    root.mainloop()


if __name__ == "__main__":
    launch_forensic_dashboard()
