# Create custom VPC

resource "google_compute_network" "custom_vpc" {
  project                 = var.project_id
  name                    = local.vpc_name
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
}

# Create private subnet in europe-west2 region

resource "google_compute_subnetwork" "mm_subnet" {
  name                     = local.subnet_name
  network                  = google_compute_network.custom_vpc.id
  region                   = var.region
  project                  = var.project_id
  ip_cidr_range            = "10.240.0.0/22"
  private_ip_google_access = true

  secondary_ip_range = [
    {
      ip_cidr_range = "10.0.0.0/14"
      range_name    = "secondary-pods-range"
    },
    {
      ip_cidr_range = "172.16.0.0/20"
      range_name    = "secondary-svc-range"
    }
  ]
}

# Create a NAT gateway to enable Dataflow Worker VMs to egress to the Internet
# to fetch Python package dependencies.
resource "google_compute_router" "nat_router" {
  name    = "mm-nat-router"
  network = google_compute_network.custom_vpc.id
  region  = var.region
  project = var.project_id

  bgp {
    asn = 64514
  }
}

resource "google_compute_router_nat" "nat_gateway" {
  name                               = "mm-nat-gateway"
  router                             = google_compute_router.nat_router.name
  region                             = var.region
  project                            = var.project_id
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
  enable_dynamic_port_allocation     = true
  min_ports_per_vm                   = 32


  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

resource "google_compute_firewall" "default_egress" {
  project   = var.project_id
  name      = "default-egress"
  network   = google_compute_network.custom_vpc.id
  direction = "EGRESS"
  priority  = 65535

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  source_ranges = [
    "10.240.0.0/22"
  ]
}

# Add firewall rule to enable Dataflow Worker VMs to communicate with each other
# This is required if the number of workers is > 1 such that they can shuffle data
# between each other.
resource "google_compute_firewall" "allow_dataflow_ports" {
  project = var.project_id
  name    = "allow-dataflow-ports"
  network = google_compute_network.custom_vpc.id

  allow {
    protocol = "tcp"
    ports    = ["12345", "12346"]
  }

  source_tags = [
    "dataflow"
  ]

  target_tags = [
    "dataflow"
  ]
}
