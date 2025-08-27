BASE_PATH = "s3://cscranalytics-prod-published/cdm/v1.0"

STACK = {
  "pie":"account=0100/project=0001",
  "stage":"account=0100/project=0002",
  "dev":"account=0100/project=0003",
  "prod":"account=5100/project=0001"
}

ORIGINATOR = {
  "imagingDevice":"com.hp.cdm.domain.telemetry.type.originatorDetail.originator.imagingDevice.version.1",
  "sirius":"com.hp.cdm.service.eventing.resource.originatorDetail.type.printer.platform.sirius.version.1",
  "ucdeSoftware":"com.hp.cdm.domain.telemetry.type.originatorDetail.originator.ucdeSoftware.version.1",
  "ucdeCloud":"com.hp.cdm.domain.telemetry.type.originatorDetail.originator.ucdeCloud.version.1",
  "ucdeWebApp":"com.hp.cdm.domain.telemetry.type.originatorDetail.originator.ucdeCloudWebApp.version.1",
  "Other":""
}

EVENT = {
  "job":"com.hp.cdm.domain.telemetry.type.eventDetail.category.job.version.1",
  "supply_sirius_service":"com.hp.cdm.service.eventing.resource.eventDetail.type.supply.platform.sirius.version.1",
  "wifiNetwork":"com.hp.cdm.domain.telemetry.type.eventDetail.category.wifiNetwork.version.3",
  "wifiNetworkSirius":"com.hp.cdm.domain.eventing.resource.eventDetail.type.wifiNetwork.platform.sirius.version.2",
  "wifiSetup":"com.hp.cdm.service.eventing.resource.eventDetail.type.wifiSetup.version.1",
  "jobStatus":"com.hp.cdm.service.eventing.resource.eventDetail.type.jobStatus.platform.sirius.version.1",
  "uuidInfo":"com.hp.cdm.service.eventing.resource.eventDetail.type.uuidInfo.platform.sirius.version.1",
  "supply_fleet_domain":"com.hp.cdm.domain.telemetry.type.eventDetail.category.supply.version.1",
  "supply_fleet_service":"com.hp.cdm.service.eventing.resource.eventDetail.type.supply.version.1",
  "infoSystemEvents":"com.hp.cdm.domain.telemetry.type.eventDetail.category.infoSystemEvents.version.1",
  "warningSystemEvents":"com.hp.cdm.domain.telemetry.type.eventDetail.category.warningSystemEvents.version.1",
  "iotOnboarding":"com.hp.cdm.domain.telemetry.type.eventDetail.category.iotOnboarding.version.1",
  "errorSystemEvents":"com.hp.cdm.domain.telemetry.type.eventDetail.category.errorSystemEvents.version.1",
  "fwUpdate":"com.hp.cdm.domain.telemetry.type.eventDetail.category.fwUpdate.version.2",
  "lifeTimeCounterSnapshot_domain":"com.hp.cdm.domain.telemetry.type.eventDetail.category.lifetimeCounterSnapshot.version.1","lifeTimeCounterSnapshot_service":"com.hp.cdm.service.eventing.resource.eventDetail.type.lifetimeCounterSnapshot.version.1",
  "rtp":"com.hp.cdm.service.eventing.resource.eventDetail.type.rtp.version.1",
  "Other":""
}

PLATFORM_FAMILY = [
  #Ares
  "LEBI","TASSEL", #Ink

  #Dune
  "MARCONI","MORETO","EDDINGTON","ELION", #Ink- MMK
  "VICTORIA", # Ink- IPH
  "ULYSSES","SELENE","ZELUS","EUTHENIA", # Laser - U/S/E/Z
  "LOTUS","CHERRY", # Laser - Flowers
  "BEAM","JUPITER","SUNSPOT", #LFP

  #SOL
  "NOVELLI","VASARI", # SOL Ink
  "MALBEC","MANHATTAN", # 3M : SOL Ink
  "HULK","STORM", #Avenger : SOL Laser
  "LOCHSA","YOSHINO", #Yolo : SOL Laser
  
  "PIXIU",
  "POSEIDON",
  "SAYAN",
  "FIREFLY",
  "DRAGONFLY",
  
  "KEYSTONE","CITRINE",

  "Other"
  ]