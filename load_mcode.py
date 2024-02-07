import requests
from tqdm import tqdm
from pathlib import Path
import argparse
import re
from urllib.parse import urljoin
import logging
import json

HEADERS = {
    "Accept-Charset": "utf-8",
    "Accept": "application/fhir+json;q=1.0, application/json+fhir;q=0.9",
    "User-Agent": "HAPI-FHIR/6.10.0 (FHIR Client; FHIR 4.0.1/R4; apache)",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/fhir+json; charset=UTF-8"
}

UUID_REGEX = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
REFERENCE_REGEX = f'"reference"\s*:\s*\"(\w+\/){UUID_REGEX}\"'


def purge_duplicates(d):
    entry = d['entry']
    unique = {each['fullUrl']: each for each in entry}.values()
    d['entry'] = list(unique)


def remove_provenance(p):
    entry = p['entry']
    for e in list(entry):
        if e['resource']['resourceType'] == 'Provenance':
            entry.remove(e)

    p['entry'] = entry


def standardize_references(p):
    dumped_p = json.dumps(p)

    def urnreplc(obj):
        return obj.group(0).replace(obj.group(1), "urn:uuid:")

    dumped_p = re.sub(REFERENCE_REGEX, urnreplc, dumped_p)
    p = json.loads(dumped_p)

    return p


def add_if_none_exist_clause(p):
    entry = p['entry']
    for i, e in enumerate(list(entry)):
        resource = e['resource']
        request = e['request']

        resource_type = resource['resourceType']

        if 'identifier' in resource.keys() and len(resource['identifier']) > 0:
            identifier = resource['identifier'][0]

        else:
            identifier = [{
                "system": "https://github.com/synthetichealth/synthea",
                "value": resource['id']
            }]

            resource['identifier'] = identifier
            identifier = identifier[0]

        system = identifier['system']
        value = identifier['value']
        query_term = f"{resource_type}?identifier={system}|{value}"
        request['ifNoneExist'] = query_term

        entry[i]['request'] = request
        entry[i]['resource'] = resource

    p['entry'] = entry


def reduce_payload_size(p):
    entry = p['entry']
    entry = entry[:10]
    p['entry'] = entry


def preprocess_payload(payload):
    # reduce_payload_size(payload)
    purge_duplicates(payload)
    remove_provenance(payload)
    payload = standardize_references(payload)
    add_if_none_exist_clause(payload)

    return payload


def upload_payload(payload_file, server):
    endpoint = urljoin(server, "fhir/")
    # endpoint = server

    with open(payload_file, 'r') as f:
        payload = json.load(f)
        payload = preprocess_payload(payload)

    resp = requests.post(endpoint, json=payload, headers=HEADERS)
    resp.raise_for_status()


def main(mcode_folder, fhir_server):
    mcode_folder = Path(mcode_folder)

    logging.info(
        f"Uploading payloads from {mcode_folder} into {fhir_server} FHIR server."
    )

    hospital_information_payloads = list(
        mcode_folder.glob("hospitalInformation*"))
    practitioner_information_payloads = list(
        mcode_folder.glob("practitionerInformation*"))

    all_fhir_payloads = list(mcode_folder.iterdir())
    patient_payloads = sorted(
        list(
            set(all_fhir_payloads) -
            (set(hospital_information_payloads)
             | set(practitioner_information_payloads))))

    pbar = tqdm(hospital_information_payloads)
    for payload_file in pbar:
        pbar.set_description(payload_file.name)
        upload_payload(payload_file, fhir_server)

    pbar = tqdm(practitioner_information_payloads)
    for payload_file in pbar:
        pbar.set_description(payload_file.name)
        upload_payload(payload_file, fhir_server)

    pbar = tqdm(patient_payloads)
    for payload_file in pbar:
        pbar.set_description(payload_file.name)
        try:
            upload_payload(payload_file, fhir_server)
        except requests.exceptions.RequestException as e:
            print(f"Failed: {payload_file}")

    logging.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load a set of mCode synthea payloads into a FHIR server.")

    parser.add_argument("mcode_folder",
                        help="Directory containing mCode synthea files.")
    parser.add_argument("fhir_server", help="FHIR Server URL")
    args = parser.parse_args()

    main(args.mcode_folder, args.fhir_server)
