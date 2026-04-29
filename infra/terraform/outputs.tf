output "public_ip_address" {
  value = azurerm_public_ip.main.ip_address
}

output "web_url" {
  value = "http://${azurerm_public_ip.main.ip_address}:${var.web_port}"
}

output "ssh_command" {
  value = "ssh ${var.admin_username}@${azurerm_public_ip.main.ip_address}"
}