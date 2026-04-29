variable "project_name" {
  type    = string
  default = "opendataai"
}

variable "resource_group_name" {
  type    = string
  default = "rg-open-data-ai-lab"
}

variable "location" {
  type    = string
  default = "westeurope"
}

variable "admin_username" {
  type    = string
  default = "azureuser"
}

variable "vm_size" {
  type    = string
  default = "Standard_B1ms"
}

variable "web_port" {
  type    = number
  default = 5000
}

variable "repo_url" {
  type    = string
  default = "https://github.com/nastiaborsch/open-data-ai-analytics.git"
}