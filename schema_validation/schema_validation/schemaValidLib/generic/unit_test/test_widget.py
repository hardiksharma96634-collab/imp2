from schemaValidLib.generic.widget import Widgets

test_widget = Widgets()
test_widget.displayWidgets()
print(test_widget.constant.base_name)
print(test_widget.constant.stack_env)
print(test_widget.constant.originator)
print(test_widget.constant.event)
print(test_widget.constant.platformFamilies)

print(test_widget.getPlatformFamily())

print(test_widget.getStackType())

print(test_widget.getEventType())

print(test_widget.getOrgType())

print(test_widget.getStartDate())

print(test_widget.getEndDate())
