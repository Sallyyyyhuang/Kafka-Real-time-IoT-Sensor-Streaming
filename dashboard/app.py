# dashboard/app.py — live Streamlit view of aggregates + alerts
import sys, os, json, threading
from collections import deque
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

st.set_page_config(page_title="IoT Sensor Stream", layout="wide")
st.title("🌡️  Live IoT Sensor Dashboard")

@st.cache_resource
def start_consumer():
    """One background consumer for the whole app session."""
    aggs, alerts = deque(maxlen=2000), deque(maxlen=200)
    def run():
        c = KafkaConsumer(
            config.TOPIC_AGGREGATES, config.TOPIC_ALERTS,
            bootstrap_servers=config.BOOTSTRAP_SERVERS,
            group_id="dashboard-2",                 # its OWN group
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        for m in c:
            (aggs if m.topic == config.TOPIC_AGGREGATES else alerts).append(m.value)
    threading.Thread(target=run, daemon=True).start()
    return aggs, alerts

aggs, alerts = start_consumer()

@st.fragment(run_every="2s")
def view():
    a = pd.DataFrame(list(aggs))
    al = pd.DataFrame(list(alerts))

    if not a.empty:
        st.subheader("Average reading per device (per window)")
        a["window_end"] = pd.to_datetime(a["window_end"], unit="s")
        chart = a.pivot_table(index="window_end", columns="device_id", values="avg")
        st.line_chart(chart)

        cols = st.columns(len(config.DEVICES))
        latest = a.sort_values("window_end").groupby("device_id").last()
        for col, (dev, row) in zip(cols, latest.iterrows()):
            col.metric(dev, f'{row["avg"]}', f'min {row["min"]} / max {row["max"]}')

    st.subheader("🚨 Alerts")
    st.dataframe(al.tail(20) if not al.empty else pd.DataFrame({"info": ["no alerts yet"]}),
                 use_container_width=True)

view()
