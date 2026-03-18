import streamlit as st
import requests
import tarfile
import yaml
import re
import pandas as pd
import json
import os
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from yaml import CSafeLoader as Loader

CACHE_FILE = "cache.json"

MAX_VERSIONS_PER_PRODUCT = 100
MAX_WORKERS = 4

PRODUCT_URLS = {
    "ET": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_et/release/",
    "Monitoring": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_monitoring/release/",
    "ADB": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_adb/release/",
    "ADQM": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_adqm/release/",
    "ADH": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_hadoop/release/",
    "ADS": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_ads/release/",
    "ADPG": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_adpg/release/",
}

ENTERPRISE_PRODUCTS = ["ADB", "ADQM", "ADH", "ADS", "ADPG"]

GROUPS = {
    "ET / Monitoring": ["ET", "Monitoring"],
    "ADB / ADQM / ADH / ADS / ADPG": ["ADB", "ADQM", "ADH", "ADS", "ADPG"],
}

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Connection": "close"
})

VERSION_RE = re.compile(r"\d+\.\d+(?:\.\d+)?")

# ---------------- CACHE ----------------

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {p: {} for p in PRODUCT_URLS}


def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=4)


# ---------------- COMMON ----------------

def open_tar_from_url(url):
    r = session.get(url, timeout=60)
    r.raise_for_status()
    return tarfile.open(fileobj=io.BytesIO(r.content), mode="r:gz")


# ---------------- VERSION CLEAN ----------------

def clean_version(v):
    if not v:
        return None
    m = VERSION_RE.search(str(v))
    return m.group(0) if m else None


# ---------------- VERSION PARSER ----------------

def extract_version(product, name):
    patterns = {
        "ET": r"_v(\d{10})",
        "Monitoring": r"_v(\d+\.\d+\.\d+)",
        "ADQM": r"_v(\d+\.\d+\.\d+\.\d+)_arenadata(\d+)",
        "ADPG": r"_v(\d+\.\d+)_arenadata(\d+)",
        "ADS": r"_v(\d+\.\d+\.\d+)_arenadata(\d+)",
    }

    pattern = patterns.get(product, r"_v(\d+\.\d+\.\d+)")
    m = re.search(pattern, name)

    if not m:
        return None

    return ".".join(m.groups())


# ---------------- BUNDLE LIST ----------------

def get_bundles(url, product):
    r = session.get(url, timeout=30)

    links = re.findall(r'href="([^"]+\.tgz[^"]*)"', r.text)

    bundles = [
        (url + href.replace("./", "")) if not href.startswith("http") else href
        for href in links
    ]

    if product in ENTERPRISE_PRODUCTS:
        bundles = [b for b in bundles if "enterprise" in b]

    return bundles


# ---------------- VERSION MAP ----------------

def build_version_map(product, bundles):
    version_map = {}

    for bundle in bundles:
        version = extract_version(product, bundle.split("/")[-1])
        if not version:
            continue

        if version not in version_map or "enterprise" in bundle:
            version_map[version] = bundle

    return dict(sorted(
        version_map.items(),
        key=lambda x: [int(i) for i in x[0].split(".")],
        reverse=True
    )[:MAX_VERSIONS_PER_PRODUCT])


# ---------------- PARSERS ----------------

def parse_et_mon(url):
    result = {"grafana": None, "graphite": None}

    with open_tar_from_url(url) as tar:
        for member in tar:

            if member.issym() or member.islnk():
                continue

            if "packs" not in member.name or not member.name.endswith(".yaml"):
                continue

            f = tar.extractfile(member)
            if not f:
                continue

            data = yaml.load(f.read(), Loader=Loader)

            images = data.get("images", [])

            for img in images:
                if "grafana" in img:
                    result["grafana"] = img.split(":")[1].split("-")[0]
                if "graphite" in img:
                    result["graphite"] = img.split(":")[1].split("_")[0]

            break

    return result


def parse_prom(url):
    result = {
        "prometheus": None,
        "pushgateway": None,
        "grafana": None,
        "node_exporter": None,
    }

    prototype_text = None
    wanted_packages = None

    with open_tar_from_url(url) as tar:
        for member in tar:

            if member.issym() or member.islnk():
                continue

            if member.name.endswith((".yaml", ".yml")):

                f = tar.extractfile(member)
                if not f:
                    continue

                try:
                    data = yaml.load(f.read(), Loader=Loader)
                except:
                    continue

                if isinstance(data, dict):

                    if "admprom_prometheus_version" in data:
                        result["prometheus"] = clean_version(data.get("admprom_prometheus_version"))

                    if "admprom_pushgateway_version" in data:
                        result["pushgateway"] = clean_version(data.get("admprom_pushgateway_version"))

                    if "admprom_grafana_version" in data:
                        result["grafana"] = clean_version(data.get("admprom_grafana_version"))

                    if "admprom_node_exporter_version" in data:
                        result["node_exporter"] = clean_version(data.get("admprom_node_exporter_version"))

            if "prototype.yaml.j2" in member.name:
                f = tar.extractfile(member)
                if f:
                    prototype_text = f.read().decode()

            if "wanted_packages.yaml" in member.name:
                f = tar.extractfile(member)
                if f:
                    try:
                        wanted_packages = yaml.load(f.read(), Loader=Loader)
                    except:
                        pass

    # ---------- prototype fallback ----------

    if prototype_text:
        prom = re.search(r"prometheus:.*?default: '([^']+)'", prototype_text, re.S)
        graf = re.search(r"grafana:.*?default: '([^']+)'", prototype_text, re.S)
        push = re.search(r"pushgateway:.*?default: '([^']+)'", prototype_text, re.S)
        node = re.search(r"node_exporter:.*?default: '([^']+)'", prototype_text, re.S)

        if prom and not result["prometheus"]:
            result["prometheus"] = prom.group(1)
        if graf and not result["grafana"]:
            result["grafana"] = graf.group(1)
        if push and not result["pushgateway"]:
            result["pushgateway"] = push.group(1)
        if node and not result["node_exporter"]:
            result["node_exporter"] = node.group(1)

    # ---------- wanted_packages fallback ----------

    def walk_yaml(node):
        if isinstance(node, list):
            for pkg in node:
                if not isinstance(pkg, dict):
                    continue

                name = (pkg.get("name") or "").lower()
                version = pkg.get("version")

                if "prometheus" in name and not result["prometheus"]:
                    result["prometheus"] = version
                elif "grafana" in name and not result["grafana"]:
                    result["grafana"] = version
                elif "node" in name and "exporter" in name:
                    result["node_exporter"] = version
                elif "pushgateway" in name and not result["pushgateway"]:
                    result["pushgateway"] = version

        elif isinstance(node, dict):
            for v in node.values():
                walk_yaml(v)

    if wanted_packages:
        walk_yaml(wanted_packages)

    return result


# ---------------- BUNDLE PROCESSOR ----------------

def process_bundle(product, version, bundle):
    try:
        parser = parse_et_mon if product in ["ET", "Monitoring"] else parse_prom
        return product, version, parser(bundle)
    except Exception as e:
        print("parse error", bundle, e)
        return product, version, None


# ---------------- UPDATE CACHE ----------------

def update_cache():
    cache = load_cache()
    progress = st.progress(0)

    tasks = [
        (p, v, b)
        for p, url in PRODUCT_URLS.items()
        for v, b in build_version_map(p, get_bundles(url, p)).items()
        if v not in cache[p] or not all(cache[p][v].values())
    ]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_bundle, *t) for t in tasks]

        for i, future in enumerate(as_completed(futures)):
            product, version, data = future.result()

            if data and any(data.values()):
                cache[product][version] = data

            if tasks:
                progress.progress((i + 1) / len(tasks))

    save_cache(cache)
    return cache


# ---------------- STREAMLIT ----------------

st.title("Versions of monitoring components for Arenadata bundles")

cache = load_cache()

if not os.path.exists(CACHE_FILE) or not any(cache.values()):
    st.info("Cache not found. Building cache...")
    cache = update_cache()
    st.success("Cache built")

if st.button("Refresh cache"):
    cache = update_cache()
    st.success("Cache updated")

tab1, tab2 = st.tabs(["ET / Monitoring", "ADB / ADQM / ADH / ADS / ADPG"])

TABS = {
    tab1: GROUPS["ET / Monitoring"],
    tab2: GROUPS["ADB / ADQM / ADH / ADS / ADPG"],
}

for tab, products in TABS.items():
    with tab:
        for product in products:
            st.subheader(product)

            data = cache.get(product, {})

            if not data:
                st.write("No data")
                continue

            df = pd.DataFrame.from_dict(data, orient="index")
            df.index.name = "Version"
            df = df.sort_index(ascending=False)

            st.dataframe(df, width="stretch")
